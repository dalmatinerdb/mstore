%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2014, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created :  4 Jun 2014 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(mstore).

-include_lib("mmath/include/mmath.hrl").
-include("mstore.hrl").

-define(SIZE_TYPE, unsigned-integer).

-define(VERSION, 2).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).
-endif.

-record(mstore, {name, file, offset, size, index=gb_trees:empty(), next=0}).
-record(mset, {size, files=[], dir, metrics=gb_sets:new()}).

-define(OPTS, [raw, binary]).
-export([put/4, get/4, new/2, delete/1, close/1, open/1, metrics/1,
         fold/3, fold/4, make_splits/3]).

%% @doc Opens an existing mstore.

delete(MSet = #mset{dir=Dir}) ->
    close(MSet),
    {ok, Files} = file:list_dir(Dir),
    Files1 = [[Dir, $/ | File] || File <- Files],
    [file:delete(F) || F <- Files1],
    file:del_dir(Dir).

-spec open(Dir :: string()) -> {ok, #mset{}} | {error, not_found}.

open(Dir) ->
    case open_mstore([Dir | "/mstore"]) of
        {ok, FileSize, Metrics} ->
            {ok, #mset{size=FileSize, dir=Dir, metrics=Metrics}};
        _ ->
            {error, not_found}
    end.

new(FileSize, Dir) when is_binary(Dir) ->
    new(FileSize, binary_to_list(Dir));

new(FileSize, Dir) ->
    IdxFile = [Dir | "/mstore"],
    case open_mstore(IdxFile) of
        {ok, F, _Metrics} when F =/= FileSize ->
            {error, filesize_missmatch};
        {ok, F, Metrics} when F =:= FileSize ->
            {ok, #mset{size=FileSize, dir=Dir, metrics=Metrics}};
        _ ->
            file:make_dir(Dir),
            MSet = #mset{size=FileSize, dir=Dir},
            file:write_file(IdxFile, <<?VERSION:16/?SIZE_TYPE,
                                       FileSize:64/?SIZE_TYPE>>),
            {ok, MSet}
    end.

open_mstore(F) ->
    case file:read_file(F) of
        {ok, <<?VERSION:16/?SIZE_TYPE, Size:64/?SIZE_TYPE, R/binary>>} ->
            {ok, Size, metrics_to_set(R, gb_sets:new())};
        E ->
            E
    end.

metrics_to_set(<<_L:16/integer, M:_L/binary, R/binary>>, S) ->
    metrics_to_set(R, gb_sets:add(M, S));

metrics_to_set(<<>>, S) ->
    S.

metrics(#mset{metrics=M}) ->
    M;

metrics(#mstore{index=M}) ->
    gb_trees:keys(M).

get(#mset{size=S, files=FS, dir=Dir}, Metric, Time, Count) when
      is_binary(Metric) ->
    ?DT_READ_ENTRY(Metric, Time, Count),
    Parts = make_splits(Time, Count, S),
    R = do_get(S, FS, Dir, Metric, Parts, <<>>),
    ?DT_READ_RETURN,
    R.

put(MSet, Metric, Time, [V0 | _] = Values)
  when is_integer(V0),
       is_integer(Time), Time >= 0,
       is_binary(Metric) ->
    put(MSet, Metric, Time, << <<?INT:?TYPE_SIZE, V:?BITS/?INT_TYPE>> || V <- Values >>);

put(MSet = #mset{size=S, files=CurFiles, metrics=Ms},
    Metric, Time, Value)
  when is_binary(Value),
       is_integer(Time), Time >= 0,
       is_binary(Metric) ->
    ?DT_WRITE_ENTRY(Metric, Time, mmath_bin:length(Value)),
    Count = mmath_bin:length(Value),
    Parts = make_splits(Time, Count, S),
    Parts1 = [{B, round(C * ?DATA_SIZE)} || {B, C} <- Parts],
    MSet1 = case gb_sets:is_element(Metric, Ms) of
                true ->
                    MSet;
                false ->
                    MSetx = MSet#mset{metrics=gb_sets:add_element(Metric, Ms)},
                    file:write_file([MSetx#mset.dir | "/mstore"],
                                    <<(byte_size(Metric)):16/integer, Metric/binary>>,
                                    [read, append]),
                    MSetx
            end,
    CurFiles1 = do_put(MSet1, Metric, Parts1, Value, CurFiles),
    ?DT_WRITE_RETURN,
    MSet1#mset{files = CurFiles1};

put(MSet, Metric, Time, V) when is_integer(V) ->
    put(MSet, Metric, Time, <<?INT:?TYPE_SIZE, V:?BITS/?INT_TYPE>>).

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

do_get(_, _, _, _, [], Acc) ->
    {ok, Acc};


do_get(S, [{FileBase, F} | _] = FS,
       Dir, Metric, [{Time, Count} | R], Acc)
  when ((Time div S)*S) =:= FileBase ->
    case read(F, Metric, Time, Count) of
        {ok, D} ->
            Acc1 = <<Acc/binary, D/binary>>,
            Acc2 = case mmath_bin:length(D) of
                       L when L < Count ->
                           Missing = Count - L,
                           <<Acc1/binary, (mmath_bin:empty(Missing))/binary>>;
                       _ ->
                           Acc1
                   end,
            do_get(S, FS, Dir, Metric, R, Acc2);
        {error,not_found} ->
            Acc1 = <<Acc/binary, (mmath_bin:empty(Count))/binary>>,
            do_get(S, FS, Dir, Metric, R, Acc1);
        eof ->
            Acc1 = <<Acc/binary, (mmath_bin:empty(Count))/binary>>,
            do_get(S, FS, Dir, Metric, R, Acc1);
        E ->
            E
    end;

do_get(S,
       [_, {FileBase, F}] = FS,
       Dir, Metric, [{Time, Count} | R], Acc)
  when ((Time div S)*S) =:= FileBase ->
    case read(F, Metric, Time, Count) of
        {ok, D} ->
            Acc1 = <<Acc/binary, D/binary>>,
            Acc2 = case mmath_bin:length(D) of
                       L when L < Count ->
                           Missing = Count - L,
                           <<Acc1/binary, (mmath_bin:empty(Missing))/binary>>;
                       _ ->
                           Acc1
                   end,
            do_get(S, FS, Dir, Metric, R, Acc2);
        {error,not_found} ->
            Acc1 = <<Acc/binary, (mmath_bin:empty(Count))/binary>>,
            do_get(S, FS, Dir, Metric, R, Acc1);
        eof ->
            Acc1 = <<Acc/binary, (mmath_bin:empty(Count))/binary>>,
            do_get(S, FS, Dir, Metric, R, Acc1);
        E ->
            E
    end;

do_get(S, FS, Dir, Metric, [{Time, Count} | R], Acc) ->
    FileBase = (Time div S)*S,
    Base = [Dir, "/", integer_to_list(FileBase)],
    {ok, F} = open(Base, FileBase, S, read),
    case read(F, Metric, Time, Count) of
        {ok, D} ->
            close(F),
            Acc1 = <<Acc/binary, D/binary>>,
            Acc2 = case mmath_bin:length(D) of
                       L when L < Count ->
                           Missing = Count - L,
                           <<Acc1/binary, (mmath_bin:empty(Missing))/binary>>;
                       _ ->
                           Acc1
                   end,
            do_get(S, FS, Dir, Metric, R, Acc2);
        {error,not_found} ->
            close(F),
            Acc1 = <<Acc/binary, (mmath_bin:empty(Count))/binary>>,
            do_get(S, FS, Dir, Metric, R, Acc1);
        eof ->
            close(F),
            Acc1 = <<Acc/binary, (mmath_bin:empty(Count))/binary>>,
            do_get(S, FS, Dir, Metric, R, Acc1);
        E ->
            close(F),
            E
    end.

make_splits(Time, Count, Size) ->
    make_splits(Time, Count, Size, []).

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


close(#mset{files=Files}) ->
    [close(S) || {_, S} <- Files],
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
    IdxFile = [File | ".idx"],
    case read_idx(IdxFile) of
        {O, S, Idx} when Offset =:= O,
                         Size =:= S ->
            case file:open([File | ".mstore"], FileOpts) of
                {ok, F} ->
                    {ok, #mstore{index=Idx, name=File, file=F, offset=Offset,
                                 size=Size, next=gb_trees:size(Idx)}};
                E ->
                    E
            end;
        {O, _, _} when Offset =/= O ->
            {error, offset_missmatch};
        {_, S, _} when Size =/= S ->
            {error, size_missmatch};
        _E ->
            case file:open([File |".mstore"], [read, write | ?OPTS]) of
                {ok, F} ->
                    M = #mstore{name=File, file=F, offset=Offset,
                                size=Size, next=0},
                    file:write_file(IdxFile,
                                    <<Offset:64/?INT_TYPE, Size:64/?INT_TYPE>>),
                    {ok, M};
                E ->
                    E
            end
    end.

open_store(File) ->
    case read_idx([File | ".idx"]) of
        {Offset, Size, Idx} ->
            case file:open([File | ".mstore"], [read | ?OPTS]) of
                {ok, F} ->
                    {ok, #mstore{index=Idx, name=File, file=F, offset=Offset,
                                 size=Size, next=gb_trees:size(Idx)}};
                E ->
                    E
            end;
        E ->
            E
    end.

write(M=#mstore{offset=Offset, size=S}, Metric, Position, Value)
  when is_binary(Value),
       Position >= Offset,
       (Position - Offset) + (byte_size(Value) div ?DATA_SIZE) =< S ->
    do_write(M, Metric, Position, Value).

do_write(M=#mstore{offset=Offset, size=S, file=F, index=Idx}, Metric, Position,
         Value) when
      is_binary(Metric),
      is_binary(Value) ->
    {M1, Base} =
        case gb_trees:lookup(Metric, Idx) of
            none ->
                Pos = M#mstore.next,
                Mx = M#mstore{next=Pos+1, index=gb_trees:insert(Metric, Pos, Idx)},
                file:write_file([M#mstore.name | ".idx"],
                                <<(byte_size(Metric)):16/integer, Metric/binary>>,
                                [read, append]),
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

fold(MSet, Fun, Acc) ->
    fold(MSet, Fun, infinity, Acc).

fold(#mset{dir=Dir}, Fun, Chunk, Acc) ->
    serialize_dir(Dir, Fun, Chunk, Acc).

serialize_dir(Dir, Fun, Chunk, Acc) ->
    {ok, Fs} = file:list_dir(Dir),
    Fs1 = [re:split(F, "\\.", [{return, list}]) || F <- Fs],
    Idxs = [I || [I, "idx"] <- Fs1],
    lists:foldl(fun(I, AccIn) ->
                        serialize_index([Dir, $/, I], Fun, Chunk, AccIn)
                end, Acc, Idxs).

serialize_index(Store, Fun, Chunk, Acc) ->
    {ok, MStore} = open_store(Store),
    Res = lists:foldl(fun(M, AccIn) ->
                              serialize_metric(MStore, M, Fun, Chunk, AccIn)
                      end, Acc, metrics(MStore)),
    close(MStore),
    Res.


serialize_metric(MStore, Metric, Fun, infinity, Acc) ->
    #mstore{offset=O,size=S} = MStore,
    Fun1 = fun(Offset, Data, AccIn) ->
                   Fun(Metric, Offset, Data, AccIn)
           end,
    {ok, Data} = read(MStore, Metric, O, S),
    serialize_binary(Data, Fun1, Acc, O, <<>>);

serialize_metric(MStore, Metric, Fun, Chunk, Acc) ->
    serialize_metric(MStore, Metric, Fun, 0, Chunk, Acc).

%% If we've read everything (Start = Size) we just return the acc
serialize_metric(#mstore{size = _Size},
                 _Metric, _Fun, _Start, _Chunk, Acc) when _Start == _Size ->
    Acc;

%% If we have at least 'Chunk' left to read
serialize_metric(MStore = #mstore{offset = O,
                                  size = Size},
                 Metric, Fun, Start, Chunk, Acc) when Start + Chunk < Size ->
    O1 = O + Start,
    Fun1 = fun(Offset, Data, AccIn) ->
                   Fun(Metric, Offset + Start, Data, AccIn)
           end,
    case read(MStore, Metric, O1, Chunk) of
        {ok, Data} ->
            Acc1 = serialize_binary(Data, Fun1, Acc, O1, <<>>),
            serialize_metric(MStore, Metric, Fun, Start + Chunk, Chunk, Acc1);
        eof ->
            Acc
    end;

%% We don't have a full chunk left to read.
serialize_metric(MStore = #mstore{offset = O,
                                  size = Size},
                 Metric, Fun, Start, _Chunk, Acc) ->
    Chunk = Size - Start,
    O1 = O + Start,
    Fun1 = fun(Offset, Data, AccIn) ->
                   Fun(Metric, Offset + Start, Data, AccIn)
           end,
    case read(MStore, Metric, O1, Chunk) of
        {ok, Data} ->
            serialize_binary(Data, Fun1, Acc, O1, <<>>);
        eof ->
            Acc
    end.


serialize_binary(<<>>, _Fun, FunAcc, _O, <<>>) ->
    FunAcc;
serialize_binary(<<>>, Fun, FunAcc, O, Acc) ->
    Fun(O, Acc, FunAcc);
serialize_binary(<<?NONE:?TYPE_SIZE, _:?BITS/?INT_TYPE, R/binary>>, Fun, FunAcc, O, <<>>) ->
    serialize_binary(R, Fun, FunAcc, O+1, <<>>);
serialize_binary(<<?NONE:?TYPE_SIZE, _:?BITS/?INT_TYPE, R/binary>>, Fun, FunAcc, O, Acc) ->
    FunAcc1 = Fun(O, Acc, FunAcc),
    serialize_binary(R, Fun, FunAcc1, O+mmath_bin:length(Acc)+1, <<>>);
serialize_binary(<<V:?DATA_SIZE/binary, R/binary>>, Fun, FunAcc, O, Acc) ->
    serialize_binary(R, Fun, FunAcc, O, <<Acc/binary, V/binary>>).

read_idx(F) ->
    case file:read_file(F) of
        {ok, <<Offset:64/?INT_TYPE, Size:64/?INT_TYPE, R/binary>>} ->
            {Offset, Size, read_idx_entreis(R, gb_trees:empty(), 0)};
        E ->
            E
    end.

read_idx_entreis(<<_L:16/integer, M:_L/binary, R/binary>>, T, I) ->
    read_idx_entreis(R, gb_trees:insert(M, I, T), I+1);
read_idx_entreis(<<>>, T, _) ->
    T.

-ifdef(TEST).

make_splits_test() ->
    T1 = 60,
    C1 = 110,
    S1 = 100,
    R1 = make_splits(T1, C1, S1),
    ?assertEqual([{60, 40}, {100, 70}], R1),
    T2 = 1401895990,
    S2 = 1000,
    C2 = 20,
    R2 = make_splits(T2, C2, S2),
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
                                        mmath_bin:to_list(R)
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
