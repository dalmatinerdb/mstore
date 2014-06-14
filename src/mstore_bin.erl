-module(mstore_bin).

-include("mstore.hrl").

-export([from_list/1, to_list/1, find_type/1, empty/1, length/1, combine/2]).

from_list([]) ->
    <<>>;

from_list([_V0 | _] = L) when is_integer(_V0) ->
    << <<?INT, V:?BITS/signed-integer>> || V <- L >>;

from_list([_V0 | _] = L) when is_float(_V0) ->
    << <<?FLOAT, V:?BITS/float>> || V <- L >>.


to_list(Bin) ->
    case find_type(Bin) of
        integer ->
            to_list_int(Bin, 0, []);
        float ->
            to_list_float(Bin, 0.0, []);
        undefined ->
            [0 || _ <- lists:seq(1, round(byte_size(Bin)/?DATA_SIZE))]
end.

to_list_int(<<?INT, I:?BITS/signed-integer, R/binary>>, _, Acc) ->
    to_list_int(R, I, [I | Acc]);
to_list_int(<<?NONE, _:?BITS/signed-integer, R/binary>>, Last, Acc) ->
    to_list_int(R, Last, [Last | Acc]);
to_list_int(<<>>, _, Acc) ->
    lists:reverse(Acc).

to_list_float(<<?FLOAT, I:?BITS/float, R/binary>>, _, Acc) ->
    to_list_float(R, I, [I | Acc]);
to_list_float(<<?NONE, _:?BITS/float, R/binary>>, Last, Acc) ->
    to_list_float(R, Last, [Last | Acc]);
to_list_float(<<>>, _, Acc) ->
    lists:reverse(Acc).


find_type(<<?INT, _:?BITS/signed-integer, _/binary>>) ->
    integer;
find_type(<<?FLOAT, _:?BITS/float, _/binary>>) ->
    float;
find_type(<<?NONE, _:?BITS/signed-integer, Rest/binary>>) ->
    find_type(Rest);
find_type(_) ->
    undefined.

length(B) ->
    trunc(byte_size(B)/?DATA_SIZE).

empty(Length) ->
    <<0:((?BITS+8)*Length)/signed-integer>>.

combine(A, B) ->
    combine(A, B, <<>>).

combine(<<?NONE, _:?BITS/signed-integer, R1/binary>>,
        <<D:?DATA_SIZE/binary, R2/binary>>,
        Acc) ->
    combine(R1, R2, <<Acc/binary, D/binary>>);
combine(<<D:?DATA_SIZE/binary, R1/binary>>,
        <<_:?DATA_SIZE/binary, R2/binary>>,
        Acc) ->
    combine(R1, R2, <<Acc/binary, D/binary>>);
combine(<<>>, <<>>, Acc) ->
    Acc;

combine(<<>>, D, Acc) ->
    <<Acc/binary, D/binary>>;
combine(D, <<>>, Acc) ->
    <<Acc/binary, D/binary>>.

