-module(mstore_bin_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("../include/mstore.hrl").

-import(mstore_heler, [int_array/0, float_array/0, non_neg_int/0, pos_int/0,
                       i_or_f_list/0, i_or_f_array/0, non_empty_i_or_f_array/0]).

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

empty_prop() ->
    ?FORALL(Length, non_neg_int(),
            byte_size(mstore_bin:empty(Length)) == Length*?DATA_SIZE).

l2b_b2l_prop() ->
    ?FORALL(List, i_or_f_list(),
            List == ?B2L(?L2B(List))).

b2l_prop() ->
    ?FORALL({_, L, B}, i_or_f_array(),
            L == ?B2L(B)).
find_type_prop() ->
    ?FORALL({T, B}, non_obvious_list(),
            T == mstore_bin:find_type(B)).


out(P) ->
   on_output(fun(S,F) -> io:format(user, S, F) end,P).

run_test_() ->
    Props = [
             fun empty_prop/0,
             fun b2l_prop/0,
             fun find_type_prop/0,
             fun l2b_b2l_prop/0
             ],
    [
     begin
         P = out(Prop()),
         ?_assert(quickcheck(numtests(500,P)))
     end
     || Prop <- Props].
