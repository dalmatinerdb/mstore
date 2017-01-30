-module(mfile_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("mmath/include/mmath.hrl").
-include("../include/mstore.hrl").

-import(mstore_helper, [int_array/0, pos_int/0, non_neg_int/0,
                        non_empty_int_list/0, defined_int_array/0,
                        size/0, non_z_int/0]).

-define(M, <<"metric">>).
-define(DIR, ".qcdata").
-compile(export_all).

new(FileSize, Offset, Dir) ->
    {ok, MF} = mfile:open(Dir ++ "/mfile", [{offset, Offset},
                                            {file_size, FileSize},
                                            {model, write},
                                            sync_bitmap]),
    MF.

mfile(FileSize, Offset) ->
    ?SIZED(Size, mfile(FileSize, Offset, Size)).


write(M, Offset, Value) ->
    Bin = mmath_bin:from_list([Value]),
    {ok, M1} = mfile:write(M, ?M, Offset, Bin),
    M1.

insert(FileSize, Offset, Size) ->
    ?LAZY(?LET({{S, T}, O, V},
               {mfile(FileSize, Offset, Size - 1),
                offset(Offset, FileSize), non_z_int()},
               {{call, ?MODULE, write, [S, O, V]},
                {call, gb_trees, enter, [O, V, T]}})).

offset(Offset, FileSize) ->
    choose(Offset, Offset + FileSize - 1).

reopen(M) ->
    mfile:close(M),
    {ok, M1} = mfile:open(filename:join([?DIR, "mfile"]),
                          [{mode, write}, sync_bitmap]),
    M1.

reopen(FileSize, Offset, Size) ->
    ?LAZY(?LET({S, T},
               mfile(FileSize, Offset, Size - 1),
               {{call, ?MODULE, reopen, [S]}, T})).

mfile(FileSize, Offset, Size) ->
    ?LAZY(oneof(
            [{{call, ?MODULE, new, [FileSize, Offset, ?DIR]},
              {call, gb_trees, empty, []}} || Size == 0]
            ++ [frequency(
                  [{9, insert(FileSize, Offset, Size)},
                   {1, reopen(FileSize, Offset, Size)}
                  ]) || Size > 0])).

offset() ->
    choose(0, 10000).

prop_bitmap() ->
    ?FORALL({FileSize, Offset}, {size(), offset()},
            begin
                ?FORALL(D, mfile(FileSize, Offset),
                        begin
                            os:cmd("rm -r " ++ ?DIR),
                            os:cmd("mkdir " ++ ?DIR),
                            {S, T} = eval(D),
                            case mfile:bitmap(S, ?M) of
                                {error, not_found} ->
                                    gb_trees:size(T) =:= 0;
                                {ok, B, S1} ->
                                    mfile:close(S1),
                                    Keys = gb_trees:keys(T),
                                    {ok, B0} = bitmap:new([{size, FileSize}]),
                                    BT = lists:foldl(
                                           fun(I, Acc) ->
                                                   {ok, Acc1} = bitmap:set(I - Offset, Acc),
                                                   Acc1
                                           end, B0, Keys),
                                    BT =:= B
                            end
                        end)
            end).
