-module(mstore_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("../include/mstore.hrl").

-import(mstore_helper, [int_array/0, pos_int/0, non_neg_int/0, size/0,
                        non_empty_int_list/0, defined_int_array/0,
                        non_z_int/0]).

-compile(export_all).
%%%-------------------------------------------------------------------
%%% Generators
%%%-------------------------------------------------------------------

-define(M, <<"metric">>).
-define(DIR, ".qcdata").

store(FileSize, MaxFiles) ->
    ?SIZED(Size, store(FileSize, Size, MaxFiles)).

max_files() ->
    non_neg_int().

insert(FileSize, Size, MaxFiles) ->
    ?LAZY(?LET({{S, T}, M, O, V},
               {store(FileSize, Size-1, MaxFiles), ?M, offset(), non_z_int()},
               {{call, mstore, put, [S, M, O, V]},
                {call, gb_trees, enter, [O, V, T]}})).

reopen(FileSize, Size, MaxFiles) ->
    ?LAZY(?LET({S, T},
               store(FileSize, Size-1, MaxFiles),
               {oneof(
                  [{call, ?MODULE, do_reindex, [S]},
                   {call, ?MODULE, renew, [S, FileSize, max_files(), ?DIR]},
                   {call, ?MODULE, do_reopen, [S, max_files(), ?DIR]}]), T})).

delete(FileSize, Size, MaxFiles) ->
    ?LAZY(?LET({{S, T}, O},
               {store(FileSize, Size-1, MaxFiles), offset()},
               {{call, ?MODULE, do_delete, [S, O]},
                {call, ?MODULE, do_delete_t, [O, FileSize, T]}})).


store(FileSize, Size, MaxFiles) ->
    ?LAZY(oneof(
            [{{call, ?MODULE, new, [FileSize, MaxFiles, ?DIR]},
              {call, gb_trees, empty, []}} || Size == 0]
            ++ [frequency(
                  [{9, insert(FileSize, Size, MaxFiles)},
                   {1, delete(FileSize, Size, MaxFiles)},
                   {1, reopen(FileSize, Size, MaxFiles)}]) || Size > 0])).

do_delete(Old, Offset) ->
    {ok, MSet} = mstore:delete(Old, Offset),
    MSet.

do_delete_t(Offset, FileSize, Tree) ->
    O1 = (Offset div FileSize) * FileSize,
    G = [{T, V} || {T, V} <- gb_trees:to_list(Tree),
                   T > O1],
    G1 = lists:sort(G),
    gb_trees:from_orddict(G1).

do_reindex(Old) ->
    {ok, MSet} = mstore:reindex(Old),
    MSet.

do_reopen(Old, MaxFiles, Dir) ->
    ok = mstore:close(Old),
    {ok, MSet} = mstore:open(Dir, [{max_files, MaxFiles}]),
    MSet.

renew(Old, FileSize, MaxFiles, Dir) ->
    ok = mstore:close(Old),
    new(FileSize, MaxFiles, Dir).

new(FileSize, MaxFiles, Dir) ->
    {ok, MSet} = mstore:new(Dir, [{file_size, FileSize},
                                  {max_files, MaxFiles}]),
    MSet.

offset() ->
    choose(0, 5000).

string() ->
    ?LET(S, ?SUCHTHAT(L, list(choose($a, $z)), L =/= ""), list_to_binary(S)).

unlist(Vs) ->
    [E] = mmath_bin:to_list(Vs),
    E.

%%%-------------------------------------------------------------------
%%% Properties
%%%-------------------------------------------------------------------
prop_read_write() ->
    ?FORALL({Metric, Size, Time, Data},
            {string(), size(), offset(), non_empty_int_list()},
            begin
                os:cmd("rm -r " ++ ?DIR),
                {ok, S1} = mstore:new(?DIR, [{file_size, Size}]),
                S2 = mstore:put(S1, Metric, Time, Data),
                {ok, Res1} = mstore:get(S2, Metric, Time, length(Data)),
                Res2 = mmath_bin:to_list(Res1),
                Metrics = btrie:fetch_keys(mstore:metrics(S2)),
                mstore:delete(S2),
                Res2 == Data andalso
                    Metrics == [Metric]
            end).

prop_read_len() ->
    ?FORALL({Metric, Size, Time, Data, TimeOffset, LengthOffset},
            {string(), size(), offset(), non_empty_int_list(), int(), int()},
            ?IMPLIES((Time + TimeOffset) > 0 andalso
                                               (length(Data) + LengthOffset) > 0,
                     begin
                         os:cmd("rm -r " ++ ?DIR),
                         {ok, S1} = mstore:new(?DIR, [{file_size, Size}]),
                         S2 = mstore:put(S1, Metric, Time, Data),
                         ReadL = length(Data) + LengthOffset,
                         ReadT = Time + TimeOffset,
                         {ok, Read} = mstore:get(S2, Metric, ReadT, ReadL),
                         mstore:delete(S2),
                         mmath_bin:length(Read) == ReadL
                     end)).

prop_gb_comp() ->
    ?FORALL({FileSize, MaxFiles}, {size(), max_files()},
            begin
                ?FORALL(D, store(FileSize, MaxFiles),
                        begin
                            os:cmd("rm -r " ++ ?DIR),
                            {S, T} = eval(D),
                            L = gb_trees:to_list(T),
                            L1 = [{mstore:get(S, ?M, T1, 1), V} || {T1, V} <- L],
                            mstore:delete(S),
                            L2 = [{unlist(Vs), Vt} || {{ok, Vs}, Vt} <- L1],
                            L3 = [true || {_V, _V} <- L2],
                            Len = length(L),
                            Res = length(L1) == Len andalso
                                length(L2) == Len andalso
                                length(L3) == Len,
                            ?WHENFAIL(io:format(user,
                                                "L:  ~p~n"
                                                "L1: ~p~n"
                                                "L2: ~p~n"
                                                "L3: ~p~n", [L, L1, L2, L3]),
                                      Res)
                        end
                       )
            end).
