%%%-------------------------------------------------------------------
%%% @author Heinz N. Gies <heinz@licenser.net>
%%% @copyright (C) 2016, Heinz N. Gies
%%% @doc MStore file module.
%%%
%%%
%%% Bitmap logic:
%%% the mfile keeps an bitmap of points written. The bitmap file
%%% structure is similar to the structure of the .mstore file
%%% as in that each element has a fixed size and is written in order
%%% based on the .idx file.
%%%
%%% The bitmap file is **not** guaranteed to be up to date, if
%%% changes are made they are made to a in memory structure an only
%%% persisted to disk when the mfile is properly closed.
%%%
%%% This can lead to inconsistencies, however the .bitmap file is
%%% just a different view on data that was already written - namely
%%% the .data file.
%%%
%%% To compesnate for posibly inconsistencies we repair the .bitmap
%%% on opening an mfile. This can be done very simply, we known the
%%% bitmap is up to date when the *lastmodified* if the bitmap file
%%% is newer then the *lastmodified* of the data file. In a sense
%%% the datafile works as a WAL for the bitmap, when our WAL is
%%% newer we know we're outdated and for the sake of 'simplicity'
%%% just recompute the entire bitmap file.
%%%
%%% While this might not be the perfect solution it also makes for
%%% a seamless update of existing mfile files, as a missing bitmap
%%% can be treated as a outdated bitmap.
%%%
%%% If mesurements show this is highly problematic we might need to
%%% adopt a more complex solution.
%%%
%%% It should be noted that last_modified returns second precisions
%%% which could lead to some unnice behaviour however we are willing
%%% to accept this lack of precision at this time.
%%%
%%% Possible problem: So far we coulde perform asyncornous reads and
%%% get data from disk without blocking other processes (yay ZFS)
%%% bitmaps get in the way. When we follow the 'write only on close'
%%% principle asyncronouys reads can not update the bitmap!
%%% Doing so would lead to the following race condiution:
%%%
%%% 1) p1 opens for write
%%% 2) p1 writes (updates BMP) min memory
%%% 3) p2 opens for reads
%%% 4) p2 finds data file older then bmp file
%%% 5) p2 updates bmp file (assumes outdated)
%%% 6) p1 closes and wants to write to bitmap
%%% 7) p1 will overwrite changes made by p2
%%%
%%% Question: Is this really problematic? P1 shoud
%%% in the worst case have a newer view on data.
%%%
%%% Possible answer: kind of ihs, it would mean p2 will update
%%% the index on nearly every read due to the data file constantly
%%% being ahead of time
%%%
%%% Possible answer: Not entirely as P1 will never write 'outdated'
%%% data as p2 won't write infomration just re-read.
%%%
%%% The corrent compromise is to only update the bitmap file on reads
%%% if no file exist (as part of a version update) and have new mstores
%%% generate an empty bitmap file on creation time.
%%% @end
%%% Created :  5 Dec 2016 by Heinz N. Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(mfile).

-export([
         open/1,
         open/2,
         metrics/1,
         size/1,
         offset/1,
         read/4,
         one_off_read/4,
         bitmap/2,
         write/4,
         close/1,
         fold/4,
         count/1,
         fold_idx/3
        ]).

-export_type([mfile/0, fold_fun/0, idx_fold_fun/0]).

-include_lib("mmath/include/mmath.hrl").

-define(OPTS, [raw, binary]).

-record(mfile, {
          name,
          file,
          offset,
          size,
          index = btrie:new(),
          next = 0,
          otime = undefined,
          bitmaps = btrie:new()
         }).
-opaque mfile() :: #mfile{}.


-type opt() :: {mode, read | write} |
               {file_size, pos_integer()} |
               {offset, non_neg_integer()}.
-type opts() :: [opt()].

-type fold_fun() :: fun((Metric :: binary(),
                         Offset :: non_neg_integer(),
                         Data :: binary(), AccIn :: any()) -> AccOut :: any()).

-type open_error_reason() :: file:posix() | badarg | system_limit |
                             offset_missmatch | size_missmatch.

-type read_error_reason() :: file:posix() | badarg | terminated | not_found.

-type read_idx_arg() :: {init, non_neg_integer(), pos_integer()} |
                        {entry, binary()}.

-type idx_fold_fun() :: fun((read_idx_arg(), AccIn :: term()) ->
                                   {ok, AccOut :: term()} | {stop, term()}).


%%--------------------------------------------------------------------
%% Public API
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc
%% Opens a metric file.
%% @end
%%--------------------------------------------------------------------

-spec open(file:filename_all()) ->
                  {ok, mfile()} |
                  {error, open_error_reason()}.
open(File) ->
    open(File, []).

-spec open(file:filename_all(), opts()) ->
                  {ok, mfile()} |
                  {error, open_error_reason()}.
open(File, Opts) ->
    Offset = case proplists:get_value(offset, Opts) of
                 OffsetX when is_integer(OffsetX), OffsetX >= 0 -> OffsetX;
                 undefined -> undefined
             end,
    Size = case proplists:get_value(file_size, Opts) of
               SizeX when is_integer(SizeX), SizeX > 0 -> SizeX;
               undefined -> undefined
           end,
    Mode = proplists:get_value(mode, Opts, read),
    FileOpts = case Mode of
                   read ->
                       [read | ?OPTS];
                   write ->
                       [read, write | ?OPTS]
               end,
    ReadIdx = read_idx(File),
    case ReadIdx of
        {O, S, Idx, Next} when (Offset =:= undefined orelse Offset =:= O),
                               (Size =:= undefined orelse Size =:= S) ->
            case file:open(File ++ ".mstore", FileOpts) of
                {ok, F} ->
                    MF = #mfile{index=Idx, name=File, file=F, offset=O,
                                size=S, next=Next},
                    {ok, check_bitmap(MF, Mode)};
                E ->
                    E
            end;
        {O, _, _, _} when Offset =/= O, Offset =/= undefined ->
            {error, offset_missmatch};
        {_, S, _, _} when Size =/= S, Size =/= undefined ->
            {error, size_missmatch};
        _E when Size =/= undefined, Offset =/= undefined  ->
            case file:open(File ++ ".mstore", [read, write | ?OPTS]) of
                {ok, F} ->
                    M = #mfile{name=File, file=F, offset=Offset,
                               size=Size, next=0},
                    file:write_file(File ++ ".idx",
                                    <<Offset:64/?INT_TYPE, Size:64/?INT_TYPE>>),
                    %% We ensure taht if we create a new mstore we also create
                    %% a bitmap file this allows us to distinguish between'
                    %% existing mstore that is outdated and bitmap was not
                    %% written.
                    file:write_file(File ++ ".bitmap", <<>>),
                    {ok, M};
                E ->
                    E
            end;
        E ->
            E
    end.

-spec metrics(mfile()) ->
                     btrie:btrie().
metrics(#mfile{index = Index}) ->
    Index.

-spec size(mfile()) ->
                  pos_integer().
size(#mfile{size = Size}) ->
    Size.

-spec offset(mfile()) ->
                    non_neg_integer().
offset(#mfile{offset = Offset}) ->
    Offset.
%%--------------------------------------------------------------------
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec read(mfile(), binary(), non_neg_integer(), pos_integer()) ->
                  {ok, binary()} |
                  {error, read_error_reason()} |
                  eof.

read(#mfile{offset=Offset, size=S, file=F, index=Idx}, Metric, Position, Count)
  when Position >= Offset,
       (Position - Offset) + Count =< S ->
    case btrie:find(Metric, Idx) of
        error ->
            {error, not_found};
        {ok, Pos} ->
            Base = Pos * S * ?DATA_SIZE,
            P = Base + ((Position - Offset) * ?DATA_SIZE),
            file:pread(F, P, Count * ?DATA_SIZE)
    end.

one_off_read(File, Metric, Position, Count) ->
    IdxRes = fold_idx(fun({init, Offset, Size}, undefined)
                            when Position >= Offset,
                                 (Position - Offset) + Count =< Size->
                              {ok, {Offset, Size, 0}};
                         ({init, _Offset, _Size}, undefined) ->
                              {stop, not_found};
                         ({entry, AMetric}, Res) when AMetric =:= Metric ->
                              {stop, {found, Res}};
                         ({entry, _M}, {Offset, Size, N}) ->
                              {ok, {Offset, Size, N + 1}}
                      end, undefined, File),
    case IdxRes of
        {found, {Offset, Size, Pos}} ->
            %% do read!
            Base = Pos * Size * ?DATA_SIZE,
            P = Base + ((Position - Offset) * ?DATA_SIZE),
            {ok, F} = file:open(File ++ ".mstore", [read | ?OPTS]),
            Res = file:pread(F, P, Count * ?DATA_SIZE),
            file:close(F),
            Res;
        _ ->
            {error, not_found}
    end.


%%--------------------------------------------------------------------
%% @doc Fetches the bitmap for a metric
%% @end
%%--------------------------------------------------------------------
-spec bitmap(mfile(), binary()) ->
                    {error, not_found} |
                    {ok, bitmap:bitmap(), mfile()}.

bitmap(M = #mfile{index = Idx}, Metric) ->
    case btrie:find(Metric, Idx) of
        error ->
            {error, not_found};
        {ok, Pos} ->
            {B, M1} = get_bitmap(Pos, M),
            {ok, B, M1}
    end.

%%--------------------------------------------------------------------
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec write(mfile(), binary(), non_neg_integer(), binary()) ->
                   {ok | {error, atom()}, mfile()}.

write(M=#mfile{offset=Offset, size=S}, Metric, Position, Value)
  when is_binary(Value),
       Position >= 0,
       Position >= Offset,
       (Position - Offset) + (byte_size(Value) div ?DATA_SIZE) =< S ->
    do_write(M, Metric, Position, Value).

%%--------------------------------------------------------------------
%% @doc
%% Closes a metric file.
%% @end
%%--------------------------------------------------------------------
-spec close(mfile()) -> ok.

close(MF = #mfile{file=F}) ->
    write_bitmap(MF),
    file:close(F).

%%--------------------------------------------------------------------
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec fold(file:filename_all() | mfile(), fold_fun(), pos_integer(), term()) ->
                  term().

fold(#mfile{} = MFile, Fun, Chunk, Acc) ->
    lists:foldl(fun(M, AccIn) ->
                        serialize_metric(MFile, M, Fun, Chunk, AccIn)
                end, Acc, get_metrics(MFile));
fold(BaseName, Fun, Chunk, Acc) ->
    {ok, MFile} = open_store(BaseName),
    Res = fold(MFile, Fun, Chunk, Acc),
    close(MFile),
    Res.

%%--------------------------------------------------------------------
%% @doc
%% Count number of elementns in give index filename
%% @end
%%--------------------------------------------------------------------
-spec count(mfile() | file:filename_all()) -> non_neg_integer().

count(#mfile{index = BT}) ->
    btrie:size(BT);
count(RootName) ->
    R = fold_idx(fun({init, _Offset, _Size}, Acc) ->
                         {ok, Acc};
                    ({entry, _M}, Acc) ->
                         {ok, Acc + 1}
                 end, 0, RootName),
    case R of
        {error, Reason} ->
            io:format("Could not read index file ~s.idx: ~p~n", [RootName, Reason]),
            0;
        N when is_number(N) ->
            N
    end.

%%--------------------------------------------------------------------
%% @doc
%% Fold over each index element in given index file.
%% @end
%%--------------------------------------------------------------------
-spec fold_idx(idx_fold_fun(), term(), string()) -> term().

fold_idx(Fun, Acc0, RootName) ->
    fold_idx(Fun, Acc0, 4*1024, RootName).


%%--------------------------------------------------------------------
%% Private functions.
%%--------------------------------------------------------------------

get_metrics(#mfile{index = M0}) ->
    M1 = lists:keysort(2, btrie:to_list(M0)),
    [M || {M, _} <- M1].

serialize_metric(MFile, Metric, Fun, infinity, Acc) ->
    #mfile{offset=O,size=S} = MFile,
    Fun1 = fun(Offset, Data, AccIn) ->
                   Fun(Metric, Offset, Data, AccIn)
           end,
    case read(MFile, Metric, O, S) of
        {ok, Data} ->
            serialize_binary(Data, Fun1, Acc, O, <<>>);
        eof ->
            Acc
    end;

serialize_metric(MFile, Metric, Fun, Chunk, Acc) ->
    serialize_metric(MFile, Metric, Fun, 0, Chunk, Acc).

%% If we've read everything (Start = Size) we just return the acc
serialize_metric(#mfile{size = _Size}, _Metric, _Fun, _Start, _Chunk, Acc)
  when _Start == _Size ->
    Acc;

%% If we have at least 'Chunk' left to read
serialize_metric(MFile = #mfile{offset = O,
                                size = Size},
                 Metric, Fun, Start, Chunk, Acc)
  when Start + Chunk < Size ->
    O1 = O + Start,
    Fun1 = fun(Offset, Data, AccIn) ->
                   Fun(Metric, Offset, Data, AccIn)
           end,
    case read(MFile, Metric, O1, Chunk) of
        {ok, Data} ->
            Acc1 = serialize_binary(Data, Fun1, Acc, O1, <<>>),
            serialize_metric(MFile, Metric, Fun, Start + Chunk, Chunk, Acc1);
        eof ->
            Acc
    end;

%% We don't have a full chunk left to read.
serialize_metric(MFile = #mfile{offset = O,
                                size = Size},
                 Metric, Fun, Start, _Chunk, Acc) ->
    Chunk = Size - Start,
    O1 = O + Start,
    Fun1 = fun(Offset, Data, AccIn) ->
                   Fun(Metric, Offset, Data, AccIn)
           end,
    case read(MFile, Metric, O1, Chunk) of
        {ok, Data} ->
            serialize_binary(Data, Fun1, Acc, O1, <<>>);
        eof ->
            Acc
    end.

serialize_binary(<<>>, _Fun, FunAcc, _O, <<>>) ->
    FunAcc;
serialize_binary(<<>>, Fun, FunAcc, O, Acc) ->
    Fun(O, Acc, FunAcc);
serialize_binary(<<?NONE:?TYPE_SIZE, R/binary>>, Fun, FunAcc, O, <<>>) ->
    VSize = ?DATA_SIZE - 1,
    <<_:VSize/binary, R1/binary>> = R,
    serialize_binary(R1, Fun, FunAcc, O+1, <<>>);
serialize_binary(<<?NONE:?TYPE_SIZE, R/binary>>, Fun, FunAcc, O, Acc) ->
    FunAcc1 = Fun(O, Acc, FunAcc),
    VSize = ?DATA_SIZE - 1,
    <<_:VSize/binary, R1/binary>> = R,
    serialize_binary(R1, Fun, FunAcc1, O+mmath_bin:length(Acc)+1, <<>>);
serialize_binary(R, Fun, FunAcc, O, Acc) ->
    <<V:?DATA_SIZE/binary, R1/binary>> = R,
    serialize_binary(R1, Fun, FunAcc, O, <<Acc/binary, V/binary>>).

open_store(BaseName) ->
    case read_idx(BaseName) of
        {Offset, Size, Idx, Next} ->
            case file:open(BaseName ++ ".mstore", [read | ?OPTS]) of
                {ok, F} ->
                    {ok, #mfile{index=Idx, name=BaseName, file=F, offset=Offset,
                                size=Size, next=Next}};
                E ->
                    E
            end;
        E ->
            io:format("Error opening file ~s.idx: ~p~n", [BaseName, E]),
            E
    end.

do_write(M=#mfile{offset=Offset, size=S, file=F, index=Idx},
         Metric, Position, Value) when
      is_binary(Metric),
      is_binary(Value) ->

    %% As part of a write operation, both the index and mstore files may be
    %% updated. There is no atomicity gaurantees, and a fault may occur after
    %% one of the writes have been processed. Writing data to the index before
    %% writing to the mstore ensures that offsets are calculated so as not to
    %% cause data to overlap. In other words, data loss is acceptable, but not
    %% data corruption.
    {M1, Pos} =
        case btrie:find(Metric, Idx) of
            error ->
                Posx = M#mfile.next,
                Mx = M#mfile{next=Posx+1, index=btrie:store(Metric, Posx, Idx)},
                Bin = <<(byte_size(Metric)):16/integer, Metric/binary>>,
                IdxFile = M#mfile.name ++ ".idx",
                ok = file:write_file(IdxFile, Bin, [read, append]),
                {Mx, Posx};
            {ok, Posx} ->
                {M, Posx}
        end,
    M2 = update_bitmap(M1, Pos, Position, Value),
    Base = Pos * S * ?DATA_SIZE,
    P = Base+((Position - Offset) * ?DATA_SIZE),
    R = file:pwrite(F, P, Value),
    {R, M2}.

update_bitmap(M = #mfile{offset = Offset}, Pos, Position, Data) ->
    {B, M1 = #mfile{bitmaps = BMPs}} = get_bitmap(Pos, M),
    B1 = set_bitmap(Data, Position - Offset, [], B),
    M1#mfile{bitmaps = btrie:store(<<Pos:32>>, B1, BMPs)}.

read_idx(BaseName) ->
    fold_idx(fun({init, Offset, Size}, undefined) ->
                     {ok, {Offset, Size, btrie:new(), 0}};
                ({entry, M}, {Offset, Size, T, I}) ->
                     {ok, {Offset, Size, btrie:store(M, I, T), I+1}}
             end, undefined, BaseName).

fold_idx(Fun, Acc0, Chunk, RootName) ->
    FileName = RootName ++ ".idx",
    case file:open(FileName, [read | ?OPTS]) of
        {ok, IO} ->
            case file:read(IO, Chunk) of
                {ok, <<Offset:64/?INT_TYPE, Size:64/?INT_TYPE, R/binary>>} ->
                    case Fun({init, Offset, Size}, Acc0) of
                        {ok, Acc} ->
                            do_fold_idx(IO, Chunk, Fun, Acc, R);
                        {stop, Acc} ->
                            file:close(IO),
                            Acc
                    end;
                _ ->
                    file:close(IO),
                    {error, invalid_file}
            end;
        E ->
            E
    end.

do_fold_idx(IO, Chunk, Fun, AccIn, <<_L:16/integer, M:_L/binary, R/binary>>) ->
    case Fun({entry, M}, AccIn) of
        {ok, Acc} ->
            do_fold_idx(IO, Chunk, Fun, Acc, R);
        {stop, Acc} ->
            file:close(IO),
            Acc
    end;
do_fold_idx(IO, Chunk, Fun, AccIn, R) ->
    case file:read(IO, Chunk) of
        {ok, Data} ->
            do_fold_idx(IO, Chunk, Fun, AccIn, <<R/binary, Data/binary>>);
        eof ->
            file:close(IO),
            case R of
                <<>> ->
                    AccIn;
                _ ->
                    {error, invalid_file}
            end;
        E ->
            file:close(IO),
            E
    end.
check_bitmap(F = #mfile{name = File}, read) ->
    case filelib:is_file(File ++ ".bitmap") of
        true ->
            %% The file exists so we don't udate
            %% it.
            F;
        false ->
            %% This is a outdated mstore that was
            %% created before we kept bitmaps
            %% even if we only read we update the
            %% bitmap
            update_bitmap(F)
    end;

check_bitmap(F = #mfile{name = File}, write) ->
    case {filelib:last_modified(File ++ ".mstore"),
          filelib:last_modified(File ++ ".bitmap")} of
        {Store, BMP} when BMP >= Store ->
            F;
        _ ->
            update_bitmap(F)
    end.

-record(acc, {
          offset,
          file,
          metric,
          bitmap,
          size,
          io
         }).

create_bitmap_fn(Metric, Idx, Data,
                 Acc = #acc{size = Size,
                            offset = Offset,
                            metric = undefined}) ->
    {ok, B} = bitmap:new([{size, Size}]),
    B1 = set_bitmap(Data, Idx - Offset, [], B),
    Acc#acc{metric = Metric, bitmap = B1};

create_bitmap_fn(Metric, Idx, Data,
                 Acc = #acc{metric = Metric,
                            offset = Offset,
                            bitmap = B}) ->
    B1 = set_bitmap(Data, Idx - Offset, [], B),
    Acc#acc{bitmap = B1};

create_bitmap_fn(Metric, Idx, Data,
                 Acc = #acc{size = Size,
                            offset = Offset,
                            bitmap = BOld,
                            io = IO}) ->
    {ok, B} = bitmap:new([{size, Size}]),
    B1 = set_bitmap(Data, Idx - Offset, [], B),
    ok = file:write(IO, BOld),
    Acc#acc{metric = Metric, bitmap = B1}.

update_bitmap(F = #mfile{name = File}) ->
    {ok, IO} = file:open(File ++ ".bitmap", [write, binary, raw]),
    Acc0 = #acc{
              io = IO,
              size = mfile:size(F),
              offset = offset(F)
             },
    #acc{bitmap = B} = fold(F, fun create_bitmap_fn/4, 4096, Acc0),
    ok = file:write(IO, B),
    ok = file:close(IO),
    update_btime(F).

write_bitmap(F = #mfile{otime = OTime, name = File, bitmaps = BMPs}) ->
    case {btrie:size(BMPs), filelib:last_modified(File ++ ".bitmap")} of
        {0, _} ->
            F;
        {_, OTimeA} when OTimeA =< OTime ->
            write_bitmap_(F);
        {_, Current} ->
            io:format("Oh my the bitmap changed since we last "
                      "read it! What shall we do?!?! For now we "
                      "just write YOLO! (read: this is stupid)\n"
                      "(Current) ~p > ~p (recorded)\n", [Current, OTime]),
            write_bitmap_(F)
    end.

write_bitmap_(F = #mfile{size = Size, bitmaps = BMPs, name = File}) ->
    BSize = bitmap:bytes(Size),
    case btrie:to_list(BMPs) of
        [] ->
            F;
        Data ->
            Writes = [{P * BSize, Bin} || {<<P:32>>, Bin} <- Data],
            {ok, IO} = file:open(File ++ ".bitmap", [raw, binary, write]),
            ok = file:pwrite(IO, Writes),
            ok = file:close(IO),
            update_btime(F)
    end.

set_bitmap(<<>>, _I, Acc, B) ->
    Acc1 = lists:reverse(Acc),
    {ok, R} = bitmap:set_many(Acc1, B),
    R;

set_bitmap(<<0, _:56, R/binary>>, I, Acc, B) ->
    set_bitmap(R, I + 1, Acc, B);

set_bitmap(<<_:64, R/binary>>, I, Acc, B) ->
    set_bitmap(R, I + 1, [I | Acc], B).

get_bitmap(Pos, M = #mfile{bitmaps = BMPs}) ->
    case btrie:find(<<Pos:32>>, BMPs) of
        {ok, B} ->
            {B, M};
        error ->
            read_bitmap(Pos, M)
    end.

read_bitmap(Pos, M = #mfile{size = Size,
                            name = File}) ->
    case file:open(File ++ ".bitmap", [raw, binary, read]) of
        {ok, IO} ->
            BSize = bitmap:bytes(Size),
            Location = Pos * BSize,
            Br = case file:pread(IO, Location, BSize) of
                     %% If the bitmap has the expected size
                     %% we return it
                     {ok, <<Size:64/unsigned, _/binary>> = Bx} ->
                         Bx;
                     %% If we can read but the size doesn't match (aka it's zero)
                     %% this is an empty bitmap and we create a new one
                     {ok, _} ->
                         {ok, Bx} = bitmap:new([{size, Size}]),
                         Bx;
                     eof ->
                         {ok, Bx} = bitmap:new([{size, Size}]),
                         Bx
                 end,
            ok = file:close(IO),
            {Br, update_btime(M)};
        _ ->
            {ok, Bx} = bitmap:new([{size, Size}]),
            {Bx, M}
    end.

update_btime(M = #mfile{name = File, otime = undefined}) ->
    OTime = filelib:last_modified(File ++ ".bitmap"),
    M#mfile{otime = OTime};

update_btime(M) ->
    M.
