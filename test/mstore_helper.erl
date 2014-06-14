-module(mstore_helper).

-include_lib("eqc/include/eqc.hrl").
-include("../include/mstore.hrl").

-export([int_array/0, float_array/0, pos_int/0, non_neg_int/0, i_or_f_list/0,
         i_or_f_array/0, non_empty_i_or_f_list/0, defined_int_array/0,
         defined_float_array/0, defined_i_or_f_array/0]).

i_or_f_array() ->
    oneof([int_array(), float_array()]).

defined_i_or_f_array() ->
    oneof([defined_int_array(), defined_float_array()]).

defined_int_array() ->
    ?SUCHTHAT({R, _, _}, int_array(), [ok || {true, _} <- R] =/= []).

defined_float_array() ->
    ?SUCHTHAT({R, _, _}, float_array(), [ok || {true, _} <- R] =/= []).

int_array() ->
    ?LET(L, list({frequency([{2, false}, {8, true}]), int()}),
         {L, to_list(L, 0, []), to_bin(L, <<>>)}).

float_array() ->
    ?LET(L, list({frequency([{2, false}, {8, true}]), real()}),
         {L, to_list_f(L), to_bin(L, <<>>)}).

pos_int() ->
    ?LET(I, int(), abs(I)+1).

non_neg_int() ->
    ?LET(I, int(), abs(I)+1).

non_empty_i_or_f_list() ->
    ?SUCHTHAT(L, i_or_f_list(), L =/= []).

i_or_f_list() ->
    oneof([list(int()), list(real())]).

to_list_f(L) ->
    case lists:keyfind(true, 1, L) of
        false ->
            to_list(L, 0, []);
        _ ->
            to_list(L, 0.0, [])
    end.

to_list([{false, _} | R], Last, Acc) ->
    to_list(R, Last, [Last | Acc]);
to_list([{true, V} | R], _, Acc) ->
    to_list(R, V, [V | Acc]);
to_list([], _, Acc) ->
    lists:reverse(Acc).

to_bin([{false, _} | R], Acc) ->
    to_bin(R, <<Acc/binary, ?NONE, 0:?BITS/signed-integer>>);

to_bin([{true, V} | R], Acc) when is_integer(V) ->
    to_bin(R, <<Acc/binary, ?INT, V:?BITS/signed-integer>>);

to_bin([{true, V} | R], Acc) when is_float(V) ->
    to_bin(R, <<Acc/binary, ?FLOAT, V:?BITS/float>>);

to_bin([], Acc) ->
    Acc.
