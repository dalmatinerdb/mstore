%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2014, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created :  4 Jun 2014 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(mstore).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(mstore, {name, file, offset, size, index=gb_trees:empty(), next=0}).

-record(mset, {size, chash, dir, seed}).
-define(DATA_SIZE, 8).
-define(OPTS, [raw, binary]).
-export([put/4, get/4, new/3, close/1, open/1, open/3, write/4, read/4, avg/3,
         sum/3, max/3, min/3, to_list/1]).

open(Dir) ->
    case file:consult(Dir++"/mstore") of
        {ok, [{FileSize, NumFiles, Seed}]} ->
            #mset{size=FileSize, chash=chash:fresh(NumFiles, []), dir=Dir, seed=Seed};
        _ ->
            {error, not_found}
    end.

new(NumFiles, FileSize, Dir) when is_binary(Dir) ->
    new(NumFiles, FileSize, binary_to_list(Dir));

new(NumFiles, FileSize, Dir) ->
    case file:consult(Dir++"/mstore") of
        {ok, [{F, N, Seed}]} when F =:= FileSize,
                                  N =:= NumFiles ->
            #mset{size=FileSize, chash=chash:fresh(NumFiles, []), dir=Dir, seed=Seed};
        {ok, _} ->
            {error, index_missmatch};
        _ ->
            Seed = erlang:phash2(now()),
            file:make_dir(Dir),
            file:write_file(Dir++"/mstore",
                            io_lib:format("~p.", [{FileSize, NumFiles, Seed}])),
            CHash = {_, Idxs} = chash:fresh(NumFiles, []),
            [file:make_dir([Dir, $/, integer_to_list(I)]) || {I, _} <- Idxs],
            #mset{size=FileSize, chash=CHash, dir=Dir, seed=Seed}
    end.

put(MSet = #mset{size=S, chash=CHash, dir=D, seed=Seed}, Metric, Time, Value) ->
    FileBase = (Time div S)*S,
    <<IndexAsInt:160/integer>> = chash:key_of({Seed, Metric}),
    Idx = chash:next_index(IndexAsInt, CHash),
    case chash:lookup(Idx, CHash) of
        [] ->
            Base = [D, $/, integer_to_list(Idx), $/, integer_to_list(FileBase)],
            {ok, F} = open(Base, FileBase, S),
            {ok, F1} = write(F, Metric, Time, Value),
            CHash1 = chash:update(Idx, [{FileBase, F1}], CHash),
            MSet#mset{chash = CHash1};
        [{FileBase, F}] ->
            {ok, F1} = write(F, Metric, Time, Value),
            CHash1 = chash:update(Idx, [{FileBase, F1}], CHash),
            MSet#mset{chash = CHash1};
        [{_Other, F}] ->
            close(F),
            Base = [D, $/, integer_to_list(Idx), $/, integer_to_list(FileBase)],
            {ok, F1} = open(Base, FileBase, S),
            {ok, F2} = write(F1, Metric, Time, Value),
            CHash1 = chash:update(Idx, [{FileBase, F2}], CHash),
            MSet#mset{chash = CHash1}
    end.

get(#mset{size=S, chash=CHash, dir=D, seed=Seed}, Metric, Time, Count) ->
    Parts = make_splits(Time, Count, S, []),
    io:format("~p~n", [Parts]),
    <<IndexAsInt:160/integer>> = chash:key_of({Seed, Metric}),
    Idx = chash:next_index(IndexAsInt, CHash),
    Dir = D ++ "/" ++ integer_to_list(Idx),
    do_get(S, Dir, Metric, Parts, <<>>).

do_get(_, _, _, [], Acc) ->
    {ok, Acc};

do_get(S, Dir, Metric, [{Time, Count} | R], Acc) ->
    FileBase = (Time div S)*S,
    Base = Dir ++ "/" ++ integer_to_list(FileBase),
    {ok, F} = open(Base, FileBase, S),
    case read(F, Metric, Time, Count) of
        {ok, D} ->
            close(F),
            do_get(S, Dir, Metric, R, <<Acc/binary, D/binary>>);
        E ->
            close(F),
            E
    end.

make_splits(_Time, 0, _Size, Acc) ->
    lists:reverse(Acc);

make_splits(Time, Count, Size, Acc) ->
    Base = (Time div Size)*Size,
    case Time - Base of
        D when (D + Count) < Size ->
            lists:reverse([{Time, Count} | Acc]);
        D ->
            Inc = Size-D,
            make_splits(Time + Inc, Count - Inc, Size, [{Time, Inc} | Acc])
    end.


close(#mstore{file=F}) ->
    file:close(F).

open(File, Offset, Size) when is_binary(File) ->
    open(binary_to_list(File), Offset, Size);

open(File, Offset, Size) ->
    case file:consult(File++".idx") of
        {ok, [{O, S, Idx}]} when Offset =:= O,
                                 Size =:= S ->
            case file:open(File++".mstore", [read, write | ?OPTS]) of
                {ok, F} ->
                    Tree=gb_trees:from_orddict(Idx),
                    {ok, #mstore{index=Tree, name=File, file=F, offset=Offset,
                                 size=Size, next=length(Idx)}};
                E ->
                    E
            end;
        {ok, [{O, _, _}]} when Offset =/= O ->
            {error, offset_missmatch};
        {ok, [{_, S, _}]} when Size =/= S ->
            {error, size_missmatch};
        {ok, _} ->
            {error, bad_index};
        _E ->
            case file:open(File++".mstore", [read, write | ?OPTS]) of
                {ok, F} ->
                    M = #mstore{name=File, file=F, offset=Offset,
                                size=Size, next=0},
                    write_index(M),
                    {ok, M};
                E ->
                    E
            end
    end.

write(M=#mstore{offset=Offset, size=S}, Metric, Position, Value)
  when is_binary(Value),
       Position >= Offset,
       (Position - Offset) + (byte_size(Value)/?DATA_SIZE) =< S ->
    do_write(M, Metric, Position, Value);

write(M=#mstore{offset=Offset, size=S}, Metric, Position, Values)
  when Position >= Offset,
       (Position - Offset) + length(Values) =< S ->
    do_write(M, Metric, Position, << <<V:64/integer>> || V <- Values >>);

write(#mstore{offset=O, size=S}, _, P, _) ->
    io:format("Out of scope: Offset:~p Size:~p Position:~p~n.", [O, S, P]),
    {error, out_of_scope}.

do_write(M=#mstore{offset=Offset, size=S, file=F, index=Idx}, Metric, Position, Value) ->
    {M1, Base} =
        case gb_trees:lookup(Metric, Idx) of
            none ->
                Pos = M#mstore.next,
                Mx = M#mstore{next=Pos+1, index=gb_trees:insert(Metric, Pos, Idx)},
                write_index(Mx),
                {Mx, Pos*S*?DATA_SIZE};
            {value, Pos} ->
                {M,Pos*S*?DATA_SIZE}
        end,
    P = Base+((Position - Offset)*?DATA_SIZE),
    R = file:pwrite(F, P, Value),
    {R, M1}.

read(#mstore{offset=Offset, size=S, file=F, index=Idx}, Metric, Position, Count)
  when Position >= Offset,
       (Position - Offset) + Count =< S ->
    case gb_trees:lookup(Metric, Idx) of
        none ->
            {error, not_found};
        {value, Pos} ->
            Base = Pos*S*?DATA_SIZE,
            P = Base+((Position - Offset)*?DATA_SIZE),
            file:pread(F, P, Count*?DATA_SIZE)
    end;

read(_,_,_,_) ->
    {error, out_of_scope}.


avg(File, Offset, Count) ->
    case file:pread(File, Offset*?DATA_SIZE, Count*8) of
        {ok, Data} ->
            calc_sum(Data, 0) / Count;
        E ->
            E
    end.

sum(File, Offset, Count) ->
    case file:pread(File, Offset*8, Count*8) of
        {ok, Data} ->
            calc_sum(Data, 0);
        E ->
            E
    end.

max(File, Offset, Count) ->
    case file:pread(File, Offset*8, Count*8) of
        {ok, <<I:64/integer, Data/binary>>} ->
            calc_max(Data, I);
        E ->
            E
    end.

min(File, Offset, Count) ->
    case file:pread(File, Offset*8, Count*8) of
        {ok, <<I:64/integer, Data/binary>>} ->
            calc_min(Data, I);
        E ->
            E
    end.

calc_sum(<<I:64/integer, R/binary>>, Sum) ->
    calc_sum(R, Sum + I);

calc_sum(<<>>, Sum) ->
    Sum.

calc_min(<<I:64/integer, R/binary>>, Min) when I < Min->
    calc_min(R, I);

calc_min(<<_:64/integer, R/binary>>, Min) ->
    calc_min(R, Min);

calc_min(<<>>, Min) ->
    Min.

calc_max(<<I:64/integer, R/binary>>, Min) when I > Min->
    calc_max(R, I);

calc_max(<<_:64/integer, R/binary>>, Min) ->
    calc_max(R, Min);

calc_max(<<>>, Min) ->
    Min.

to_list(Bin) ->
    [I || <<I:64/integer>> <= Bin].

write_index(#mstore{name=F, index=I, offset=O, size=S}) ->
    file:write_file(F ++ ".idx", io_lib:format("~p.", [{O, S, gb_trees:to_list(I)}])).

-ifdef(TEST).

make_splits_test() ->
    T1 = 60,
    C1 = 110,
    S1 = 100,
    R1 = make_splits(T1, C1, S1, []),
    ?assertEqual([{60, 40}, {100, 70}], R1),
    T2 = 1401895990,
    S2 = 1000,
    C2 = 20,
    R2 = make_splits(T2, C2, S2, []),
    ?assertEqual([{1401895990, 10}, {1401896000, 10}], R2).


bench_test_() ->
    NumMetrics = 1000,
    Dir = "bench",
    T0 = 600,
    NumPoints = 1000,
    file:make_dir(Dir),
    S = mstore:new(20, 1000, Dir),
    Metrics = [list_to_binary(io_lib:format("metric~p", [I])) || I <- lists:seq(0, NumMetrics)],
    {T, S1} = timer:tc(fun () ->
                               add_points(S, Metrics, T0, NumPoints)
                       end),
    Seconds = T / 1000000,
    TotalInserts = NumMetrics*NumPoints,
    ?debugFmt("Inserted ~p metrics in ~p seconds meaning ~p metrics/second.",
              [TotalInserts, Seconds, TotalInserts/Seconds]),
    [M0|_] = Metrics,
    {ok, R} = get(S1, M0, 610, 10),
    L = to_list(R),
    {timeout, 15,
     ?_assertEqual(lists:reverse(lists:seq(NumPoints-19, NumPoints-10)), L)}.

add_points(S, _, _, 0) ->
    S;
add_points(S, Metrics, T, Ps) ->
    S1 = lists:foldl(fun(M, SAcc) ->
                             put(SAcc, M, T, [Ps])
                     end, S, Metrics),
    add_points(S1, Metrics, T+1, Ps-1).


-endif.
