-module(mstore_aggr_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("../include/mstore.hrl").

-import(mstore_heler, [int_array/0, float_array/0, pos_int/0, non_neg_int/0,
                       i_or_f_list/0, i_or_f_array/0,
                       non_empty_i_or_f_list/0, out/1]).

-compile(export_all).

n_length_chunks_prop() ->
    ?FORALL({L, N}, {list(int()), pos_int()},
            ceiling(length(L) / N) =:= length(n_length_chunks(L, N))).

avg_all_prop() ->
    ?FORALL(L, non_empty_i_or_f_list(),
            [lists:sum(L)/length(L)] == ?B2L(mstore_aggr:avg(?L2B(L), length(L)))).

avg_len_prop() ->
    ?FORALL({L, N}, {non_empty_i_or_f_list(), pos_int()},
            ceiling(length(L)/N) == length(?B2L(mstore_aggr:avg(?L2B(L), N)))).

avg_impl_prop() ->
    ?FORALL({{_, L, B}, N}, {i_or_f_array(), pos_int()},
            avg(L, N) == mstore_bin:to_list(mstore_aggr:avg(B, N))).

sum_prop() ->
    ?FORALL({{_, L, B}, N}, {i_or_f_array(), pos_int()},
            sum(L, N) == mstore_bin:to_list(mstore_aggr:sum(B, N))).

der_prop() ->
    ?FORALL({_, L, B}, i_or_f_array(),
            derivate(L) == mstore_bin:to_list(mstore_aggr:derivate(B))).

avg(L, N) ->
    apply_n(L, N, fun avg_/2).

sum(L, N) ->
    apply_n(L, N, fun sum_/2).

apply_n(L, N, F) ->
    [F(SL, N) || SL <- n_length_chunks(L, N)].

avg_(L, N) ->
    lists:sum(L) / N.

sum_(L, _N) ->
    lists:sum(L).

derivate([]) ->
    [];
derivate([H | T]) ->
    derivate(H, T, []).
derivate(H, [H1 | T], Acc) ->
    derivate(H1, T, [H1 - H | Acc]);
derivate(_, [], Acc) ->
    lists:reverse(Acc).

ceiling_prop() ->
    ?FORALL(F, real(),
            F =< ceiling(F)).

%% Taken from: http://schemecookbook.org/Erlang/NumberRounding
ceiling(X) ->
    T = erlang:trunc(X),
    case (X - T) of
        Neg when Neg < 0 -> T;
        Pos when Pos > 0 -> T + 1;
        _ -> T
    end.
%% taken from http://stackoverflow.com/questions/12534898/splitting-a-list-in-equal-sized-chunks-in-erlang
n_length_chunks([],_) -> [];
n_length_chunks(List,Len) when Len > length(List) ->
    [List];
n_length_chunks(List,Len) ->
    {Head,Tail} = lists:split(Len,List),
    [Head | n_length_chunks(Tail,Len)].

run_test_() ->
    Props = [
             fun n_length_chunks_prop/0,
             fun avg_all_prop/0,
             fun avg_len_prop/0,
             fun avg_impl_prop/0,
             fun sum_prop/0,
             fun der_prop/0,
             fun ceiling_prop/0
             ],
    [
     begin
         P = out(Prop()),
         ?_assert(quickcheck(numtests(500,P)))
     end
     || Prop <- Props].
