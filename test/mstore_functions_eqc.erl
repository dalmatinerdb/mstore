-module(mstore_functions_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("../include/mstore.hrl").

-import(mstore_heler, [int_array/0, float_array/0, pos_int/0, non_neg_int/0,
                       i_or_f_list/0, i_or_f_array/0, non_empty_i_or_f_list/0]).

-compile(export_all).
%%%-------------------------------------------------------------------
%%% Generators
%%%-------------------------------------------------------------------

size() ->
    choose(1000,2000).

length() ->
    choose(200, 5000).

offset() ->
    choose(0, 5000).

%%%-------------------------------------------------------------------
%%% Properties
%%%-------------------------------------------------------------------
prop_cout_parts() ->
    ?FORALL({Size, Time, Count}, {size(), offset(), length()},
            lists:sum([C || {_, C} <-mstore:make_splits(Time, Count, Size, [])]) == Count).

prop_max_size_make_splits() ->
    ?FORALL({Size, Time, Count}, {size(), offset(), length()},
            length([false || {_, C} <-mstore:make_splits(Time, Count, Size, []),
                   C > Size]) == 0).

prop_incomplete_make_splits() ->
    ?FORALL({Size, Time, Count}, {size(), offset(), length()},
            length([false || {_, C} <-mstore:make_splits(Time, Count, Size, []),
                   C < Size]) =< 2).

-include("eqc_helper.hrl").
