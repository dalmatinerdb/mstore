-module(mstore_bin_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("../include/mstore.hrl").

-import(mstore_helper, [int_array/0, float_array/0, non_neg_int/0, pos_int/0,
                       i_or_f_list/0, i_or_f_array/0, out/1]).

-compile(export_all).

non_obvious_list() ->
    oneof([
          ?LET({N, L}, {non_neg_int(), list(int())},
               oneof(
                 [{integer, <<(mstore_bin:empty(N))/binary, (mstore_bin:from_list(L))/binary>>} || L =/= []] ++
                     [{undefined, <<(mstore_bin:empty(N))/binary, (mstore_bin:from_list(L))/binary>>} || L == []])),
          ?LET({N, L}, {non_neg_int(), list(real())},
               oneof(
                 [{float, <<(mstore_bin:empty(N))/binary, (mstore_bin:from_list(L))/binary>>} || L =/= []] ++
                     [{undefined, <<(mstore_bin:empty(N))/binary, (mstore_bin:from_list(L))/binary>>} || L == []]))]).

prop_empty() ->
    ?FORALL(Length, non_neg_int(),
            byte_size(mstore_bin:empty(Length)) == Length*?DATA_SIZE).

prop_length() ->
    ?FORALL(Length, non_neg_int(),
            mstore_bin:length(mstore_bin:empty(Length)) == Length).

prop_l2b_b2l() ->
    ?FORALL(List, i_or_f_list(),
            List == ?B2L(?L2B(List))).

prop_b2l() ->
    ?FORALL({_, L, B}, i_or_f_array(),
            L == ?B2L(B)).

prop_find_type() ->
    ?FORALL({T, B}, non_obvious_list(),
            T == mstore_bin:find_type(B)).

prop_combine_int() ->
    ?FORALL({{La, _, Ba}, {Lb, _, Bb}}, {int_array(), int_array()},
            combine(La, Lb) == mstore_bin:to_list(mstore_bin:combine(Ba, Bb))).

prop_combine_float() ->
    ?FORALL({{La, _, Ba}, {Lb, _, Bb}}, {float_array(), float_array()},
            combine(La, Lb) == mstore_bin:to_list(mstore_bin:combine(Ba, Bb))).

combine(A, B) ->
    combine(A, B, []).

combine([{false, _} | R1], [{true, V} | R2], Acc) ->
    combine(R1, R2, [V | Acc]);
combine([{true, V} | R1], [_ | R2], Acc) ->
    combine(R1, R2, [V | Acc]);
combine([_ | R1], [_ | R2], [Last | _] = Acc) ->
    combine(R1, R2, [Last | Acc]);
combine([_ | R1], [_ | R2], []) ->
    combine(R1, R2, [0]);
combine([], [], Acc) ->
    lists:reverse(Acc);
combine([], [{true, V} | R], Acc ) ->
    combine([], R, [V | Acc]);
combine([], [{false, _} | R], []) ->
    combine([], R, [0]);
combine([], [{false, _} | R], [Last | _] = Acc) ->
    combine([], R, [Last | Acc]);
combine(A, [], Acc) ->
    combine([], A, Acc).





-include("eqc_helper.hrl").
