%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2014, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created :  4 Jun 2014 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(mstore).

-include("mstore.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).
-endif.

-record(mstore, {name, file, offset, size, index=gb_trees:empty(), next=0}).
-record(mset, {size, chash, dir, seed, metrics=gb_sets:new()}).

-define(OPTS, [raw, binary]).
-export([put/4, get/4, new/3, delete/1, close/1, open/1, metrics/1,
         fold/3]).

%% @doc Opens an existing mstore.

delete(MSet = #mset{dir=Dir, chash=CHash}) ->
    close(MSet),
    Buckets = [[Dir, $/ | integer_to_list(I)] || {I, _} <- chash:nodes(CHash)],
    [delete_chash_bucket(Bucket) || Bucket <- Buckets],
    file:delete([Dir, $/ | "mstore"]),
    file:del_dir(Dir).

delete_chash_bucket(Dir) ->
    {ok, Files} = file:list_dir(Dir),
    Files1 = [[Dir, $/ | File] || File <- Files],
    [file:delete(F) || F <- Files1],
    file:del_dir(Dir).

-spec open(Dir :: string()) -> {ok, #mset{}} | {error, not_found}.

open(Dir) ->
    case file:consult([Dir | "/mstore"]) of
        {ok, [{FileSize, NumFiles, Seed, Metrics}]} ->
            {ok, #mset{size=FileSize, chash=chash:fresh(NumFiles, []), dir=Dir,
                       seed=Seed, metrics=gb_sets:from_list(Metrics)}};
        _ ->
            {error, not_found}
    end.

new(NumFiles, FileSize, Dir) when is_binary(Dir) ->
    new(NumFiles, FileSize, binary_to_list(Dir));

new(NumFiles, FileSize, Dir) ->
    case file:consult([Dir | "/mstore"]) of
        {ok, [{F, _N, _Seed, _Metrics}]} when F =/= FileSize ->
            {error, filesize_missmatch};
        {ok, [{_F, N, _Seed, _Metrics}]} when N =/= NumFiles ->
            {error, ring_size_missmatch};
        {ok, [{F, N, Seed, Metrics}]} when F =:= FileSize,
                                           N =:= NumFiles ->
            {ok, #mset{size=FileSize, chash=chash:fresh(NumFiles, []), dir=Dir,
                       seed=Seed, metrics=gb_sets:from_list(Metrics)}};
        {ok, I} ->
            {error, {bad_index, I}};
        _ ->
            Seed = erlang:phash2(now()),
            file:make_dir(Dir),
            CHash = {_, Idxs} = chash:fresh(NumFiles, []),
            MSet = #mset{size=FileSize, chash=CHash, dir=Dir, seed=Seed},
            save_set(MSet),
            [file:make_dir([Dir, $/ | integer_to_list(I)]) || {I, _} <- Idxs],
            {ok, MSet}
    end.

save_set(#mset{dir=D, size=Size, chash=CHash, seed=Seed,metrics=Metrics}) ->
    NumFiles = chash:size(CHash),
    file:write_file([D | "/mstore"],
                    io_lib:format("~p.", [{Size, NumFiles, Seed,
                                           gb_sets:to_list(Metrics)}])).


metrics(#mset{metrics=M}) ->
    M;

metrics(#mstore{index=M}) ->
    gb_trees:keys(M).

put(MSet, Metric, Time, [V0 | _] = Values)
  when is_integer(V0) ->
    put(MSet, Metric, Time, << <<?INT, V:?BITS/integer>> || V <- Values >>);

put(MSet, Metric, Time, [V0 | _] = Values)
  when is_float(V0) ->
    put(MSet, Metric, Time, << <<?FLOAT, V:?BITS/float>> || V <- Values >>);

put(MSet = #mset{size=S, chash=CHash, seed=Seed, dir=D, metrics=Ms},
    Metric, Time, Value)
  when is_binary(Value)
       ->
    <<IndexAsInt:160/integer>> = chash:key_of({Seed, Metric}),
    Idx = chash:next_index(IndexAsInt, CHash),
    Count = byte_size(Value) / ?DATA_SIZE,
    Parts = make_splits(Time, Count, S, []),
    CurFiles = chash:lookup(Idx, CHash),
    Parts1 = [{B, round(C*?DATA_SIZE)} || {B, C} <- Parts],
    MSet1 = case gb_sets:is_element(Metric, Ms) of
                true ->
                    MSet;
                false ->
                    MSetx = MSet#mset{metrics=gb_sets:add_element(Metric, Ms)},
                    save_set(MSetx),
                    MSetx
            end,
    case do_put(MSet1#mset{dir=[D, $/, integer_to_list(Idx)]}, Metric, Parts1, Value, CurFiles) of
        CurFiles1 when CurFiles1 =:= CurFiles ->
            MSet1;
        CurFiles1 ->
            CHash1 = chash:update(Idx, CurFiles1, CHash),
            MSet1#mset{chash = CHash1}
    end;


put(MSet, Metric, Time, V) when is_integer(V) ->
    put(MSet, Metric, Time, <<?INT, V:?BITS/integer>>);

put(MSet, Metric, Time, V) when is_float(V) ->
    put(MSet, Metric, Time, <<?FLOAT, V:?BITS/float>>).

do_put(_, _, [], <<>>, Files) ->
    Files;

do_put(MSet = #mset{size=S}, Metric,
       [{Time, Size} | R], InData,
       [{FileBase, F} | FileRest]) when
      ((Time div S)*S) =:= FileBase ->
    <<Data:Size/binary, DataRest/binary>> = InData,
    {ok, F1} = write(F, Metric, Time, Data),
    do_put(MSet, Metric, R, DataRest, [{FileBase, F1} | FileRest]);

do_put(MSet = #mset{size=S}, Metric,
       [{Time, Size} | R], InData,
       [First, {FileBase, F}]) when
      ((Time div S)*S) =:= FileBase ->
    <<Data:Size/binary, DataRest/binary>> = InData,
    {ok, F1} = write(F, Metric, Time, Data),
    do_put(MSet, Metric, R, DataRest, [{FileBase, F1}, First]);

do_put(MSet = #mset{size=S, dir=D}, Metric,
       [{Time, Size} | R], InData,
       [First, {_Other, F}]) ->
    <<Data:Size/binary, DataRest/binary>> = InData,
    FileBase = (Time div S)*S,
    close(F),
    Base = [D, $/, integer_to_list(FileBase)],
    {ok, F1} = open(Base, FileBase, S, write),
    {ok, F2} = write(F1, Metric, Time, Data),
    do_put(MSet, Metric, R, DataRest, [{FileBase, F2}, First]);

do_put(MSet = #mset{size=S, dir=D}, Metric,
       [{Time, Size} | R], InData,
       Files) when length(Files) < 2 ->
    <<Data:Size/binary, DataRest/binary>> = InData,
    FileBase = (Time div S)*S,
    Base = [D, $/, integer_to_list(FileBase)],
    {ok, F1} = open(Base, FileBase, S, write),
    {ok, F2} = write(F1, Metric, Time, Data),
    do_put(MSet, Metric, R, DataRest, [{FileBase, F2} | Files]).

get(#mset{size=S, chash=CHash, dir=D, seed=Seed}, Metric, Time, Count) ->
    Parts = make_splits(Time, Count, S, []),
    <<IndexAsInt:160/integer>> = chash:key_of({Seed, Metric}),
    Idx = chash:next_index(IndexAsInt, CHash),
    Dir = [D, "/", integer_to_list(Idx)],
    do_get(S, Dir, Metric, Parts, <<>>).

do_get(_, _, _, [], Acc) ->
    {ok, Acc};

do_get(S, Dir, Metric, [{Time, Count} | R], Acc) ->
    FileBase = (Time div S)*S,
    Base = [Dir, "/", integer_to_list(FileBase)],
    {ok, F} = open(Base, FileBase, S, read),
    case read(F, Metric, Time, Count) of
        {ok, D} ->
            close(F),
            Acc1 = <<Acc/binary, D/binary>>,
            Acc2 = case mstore_bin:length(D) of
                       L when L < Count ->
                           Missing = Count - L,
                           <<Acc1/binary, (mstore_bin:empty(Missing))/binary>>;
                       _ ->
                           Acc1
                   end,
            do_get(S, Dir, Metric, R, Acc2);
        {error,not_found} ->
            close(F),
            Acc1 = <<Acc/binary, (mstore_bin:empty(Count))/binary>>,
            do_get(S, Dir, Metric, R, Acc1);
        eof ->
            close(F),
            Acc1 = <<Acc/binary, (mstore_bin:empty(Count))/binary>>,
            do_get(S, Dir, Metric, R, Acc1);
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


close(#mset{chash=CHash}) ->
    Nodes = chash:nodes(CHash),
    Stores = lists:flatten([V || {_K, V} <- Nodes]),
    Stores1 = [V || {_K, V} <- Stores],
    [close(S) || S <- Stores1],
    ok;
close(#mstore{file=F}) ->
    file:close(F).

open(File, Offset, Size, Mode) ->
    FileOpts = case Mode of
                   read ->
                       [read | ?OPTS];
                   write ->
                       [read, write | ?OPTS]
               end,
    case file:consult([File | ".idx"]) of
        {ok, [{O, S, Idx}]} when Offset =:= O,
                                 Size =:= S ->
            case file:open([File | ".mstore"], FileOpts) of
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
            case file:open([File |".mstore"], [read, write | ?OPTS]) of
                {ok, F} ->
                    M = #mstore{name=File, file=F, offset=Offset,
                                size=Size, next=0},
                    write_index(M),
                    {ok, M};
                E ->
                    E
            end
    end.

open_store(File) ->
    case file:consult([File | ".idx"]) of
        {ok, [{Offset, Size, Idx}]} ->
            case file:open([File | ".mstore"], [read | ?OPTS]) of
                {ok, F} ->
                    Tree=gb_trees:from_orddict(Idx),
                    {ok, #mstore{index=Tree, name=File, file=F, offset=Offset,
                                 size=Size, next=length(Idx)}};
                E ->
                    E
            end;
        E ->
            E
    end.

write(M=#mstore{offset=Offset, size=S}, Metric, Position, Value)
  when is_binary(Value),
       Position >= Offset,
       (Position - Offset) + (byte_size(Value)/?DATA_SIZE) =< S ->
    do_write(M, Metric, Position, Value).

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
    end.

write_index(#mstore{name=F, index=I, offset=O, size=S}) ->
    file:write_file([F | ".idx"], io_lib:format("~p.", [{O, S, gb_trees:to_list(I)}])).

fold(#mset{dir=Dir, chash=CHash}, Fun, Acc) ->
    Dirs = [Dir ++ [$/ | integer_to_list(I)] || {I, _} <- chash:nodes(CHash)],
    lists:foldl(fun(D, AccIn) ->
                        serialize_dir(D, Fun, AccIn)
                end, Acc, Dirs).

serialize_dir(Dir, Fun, Acc) ->
    {ok, Fs} = file:list_dir(Dir),
    Fs1 = [re:split(F, "\\.", [{return, list}]) || F <- Fs],
    Idxs = [I || [I, "idx"] <- Fs1],
    lists:foldl(fun(I, AccIn) ->
                        serialize_index([Dir, $/, I], Fun, AccIn)
                end, Acc, Idxs).

serialize_index(Store, Fun, Acc) ->
    {ok, MStore} = open_store(Store),
    lists:foldl(fun(M, AccIn) ->
                        serialize_metric(MStore, M, Fun, AccIn)
                end, Acc, metrics(MStore)).


serialize_metric(MStore, Metric, Fun, Acc) ->
    #mstore{offset=O,size=S} = MStore,
    Fun1 = fun(Offset, Data, AccIn) ->
                   Fun(Metric, Offset, Data, AccIn)
           end,
    {ok, Data} = read(MStore, Metric, O, S),
    serialize_binary(Data, Fun1, Acc, O, <<>>).

serialize_binary(<<>>, _Fun, FunAcc, _O, <<>>) ->
    FunAcc;
serialize_binary(<<>>, Fun, FunAcc, O, Acc) ->
    Fun(O, Acc, FunAcc);
serialize_binary(<<?NONE, _:?BITS/integer, R/binary>>, Fun, FunAcc, O, <<>>) ->
    serialize_binary(R, Fun, FunAcc, O+1, <<>>);
serialize_binary(<<?NONE, _:?BITS/integer, R/binary>>, Fun, FunAcc, O, Acc) ->
    FunAcc1 = Fun(O, Acc, FunAcc),
    serialize_binary(R, Fun, FunAcc1, O+mstore_bin:length(Acc)+1, <<>>);
serialize_binary(<<V:?DATA_SIZE/binary, R/binary>>, Fun, FunAcc, O, Acc) ->
    serialize_binary(R, Fun, FunAcc, O, <<Acc/binary, V/binary>>).


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

-ifdef(BENCH).
bench_test_() ->
    {timeout, 60,
     fun() ->
             NumMetrics = 100,
             Dir = "bench",
             T0 = 450,
             NumPoints = 2500,
             Size = 10,
             file:make_dir(Dir),
             {ok, S} = mstore:new(20, 200, Dir),
             Metrics = [list_to_binary(io_lib:format("metric~p", [I])) || I <- lists:seq(0, NumMetrics)],
             {T, S1} = timer:tc(fun () ->
                                        add_points(S, Metrics, Size, T0, NumPoints)
                                end),
             Seconds = T / 1000000,
             TotalInserts = NumMetrics*NumPoints*Size,
             ?debugFmt("Inserted ~p metrics in batches of ~p, this took ~p seconds meaning ~p metrics/second.",
                       [TotalInserts, Size, Seconds, TotalInserts/Seconds]),
             [M0|_] = Metrics,
             {T1, {ok, R}} = timer:tc(fun() ->
                                              get(S1, M0, T0, NumPoints)
                                      end),
             Seconds1 = T1 / 1000000,
             ?debugFmt("Read ~p metrics in ~p seconds meaning ~p metrics/second.",
                       [NumPoints, Seconds1, NumPoints/Seconds1]),
             {T2, L} = timer:tc(fun() ->
                                        mstore_bin:to_list(R)
                                end),
             Seconds2 = T2 / 1000000,
             ?debugFmt("Converted ~p metrics in ~p seconds meaning ~p metrics/second.",
                       [NumPoints, Seconds2, NumPoints/Seconds2]),
             Expected = lists:reverse(lists:seq(NumPoints*9+1, NumPoints*10)),
             ?assertEqual(length(Expected), length(L)),
             ?assertEqual(Expected, L)
     end}.

add_points(S, Metrics, _Size, T, 0) ->
    S1 = lists:foldl(fun(M, SAcc) ->
                             put(SAcc, M, T, [0])
                     end, S, Metrics),
    S1;

add_points(S, Metrics, Size, T, Ps) ->
    Data = lists:reverse(lists:seq((Ps-1)*Size, Ps*Size)),
    S1 = lists:foldl(fun(M, SAcc) ->
                             put(SAcc, M, T, Data)
                     end, S, Metrics),
    add_points(S1, Metrics, Size, T+Size, Ps-1).
-endif.

-endif.
