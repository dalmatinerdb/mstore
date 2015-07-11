-module(mstore_helper).

-include_lib("eqc/include/eqc.hrl").
-include_lib("mmath/include/mmath.hrl").
-include("../include/mstore.hrl").

-export([int_array/0, pos_int/0, non_neg_int/0, non_empty_int_list/0,
         defined_int_array/0]).

defined_int_array() ->
    ?SUCHTHAT({R, _, _}, int_array(), [ok || {true, _} <- R] =/= []).

int_array() ->
    ?LET(L, list({frequency([{2, false}, {8, true}]), int()}),
         {L, to_list(L, 0, []), to_bin(L, <<>>)}).

pos_int() ->
    ?LET(I, int(), abs(I)+1).

non_neg_int() ->
    ?LET(I, int(), abs(I)+1).

non_empty_int_list() ->
    ?SUCHTHAT(L, list(int()), L =/= []).

to_list([{false, _} | R], Last, Acc) ->
    to_list(R, Last, [Last | Acc]);
to_list([{true, V} | R], _, Acc) ->
    to_list(R, V, [V | Acc]);
to_list([], _, Acc) ->
    lists:reverse(Acc).

to_bin([{false, _} | R], Acc) ->
    to_bin(R, <<Acc/binary, ?NONE:?TYPE_SIZE, 0:?BITS/?INT_TYPE>>);

to_bin([{true, V} | R], Acc) when is_integer(V) ->
    to_bin(R, <<Acc/binary, ?INT:?TYPE_SIZE, V:?BITS/?INT_TYPE>>);

to_bin([], Acc) ->
    Acc.
