%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2014, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created :  4 Jun 2014 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------

%% @doc An mstore instance is per directory, consisting of multiple `mstore'
%% and `idx' files. Data points are stored according to their timestamp, and
%% stores typically retain up to one week's (depends on `file_size') worth of
%% data. One master `mstore' file will be created for all metric paths in the
%% directory, and is used for coverage queries.
%%
%% Index (`idx') files are keyed on and offset position and consist of metric
%% names.  These metrics are used to calculate the relative position of a
%% metric's values in the `mstore' storage file.
%%
%% The format of an `idx' file is as follows, where `M..' are metric paths:
%% +--------+-----------+----------+----+----------+----+-----+----------+----+
%% | Offset | File Size | Size(M1) | M1 | Size(M2) | M2 | ... | Size(Mn) | Mn |
%% +--------+-----------+----------+----+----------+----+-----+----------+----+
%%                      |---- Pos 0 ----|---- Pos 2 ----|     |---- Pos n ----|
%%
%% Metric names are appended to the index the first time that they are
%% encountered.
%%
%% Storage (mstore) files may store values for more than one metric at a time,
%% and is segmented sequentially.  The position for a value within a
%% segment is calculated as follows: P = Base + ((TS - Offset) * Data size),
%% where Base is a function of the metric's position in the index:
%% +-------------+-----+------------------------------------------------+
%% | Pos(M1) = 0 | --> | Base = 0 X File size X Data size = 0           |
%% | Pos(M2) = 1 | --> | Base = 1 X File size X Data size = 4838400     |
%% | ...         |     |                                                |
%% | Pos(Mn) = N | --> | Base = N X File size X Data size = N * 4838400 |
%% +-------------+-----+------------------------------------------------+
%%
%% Only one process should open the mstore for writing at a given time. At any
%% moment, up to two `mstore' files may be open for writing by the mstore.
%% These files represent the current and previously written to files e.g. the
%% current and previous week.

-module(mstore).

-export([
         clone/1,
         new/2,
         open/1,
         open/2,
         close/1,
         delete/1,
         delete/2,
         reindex/1,
         get/4,
         get/5,
         bitmap/3,
         put/4,
         metrics/1,
         count/1,
         file_size/1,
         fold/3, fold/4,
         make_splits/3]).
-export_type([mstore/0]).


-include_lib("mmath/include/mmath.hrl").
-include("mstore.hrl").

-define(SIZE_TYPE, unsigned-integer).

-define(VERSION, 4).
-define(OPTS, [raw, binary]).
-define(MFILE_VER, 1).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).
-endif.

-record(mstore, {
          size              :: pos_integer(),
          max_files = 2     :: non_neg_integer(),
          files = []        :: [{non_neg_integer(), mfile:mfile()}],
          dir               :: string(),
          metrics = delayed :: btrie:btrie() | delayed
         }).

-opaque mstore() :: #mstore{}.

-type open_opt() ::
        {max_files, pos_integer()} | preload_index.

-type new_opt() ::
        {file_size, pos_integer()}.

-type read_opt() :: one_off.

%%--------------------------------------------------------------------
%% Public API
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc
%% Creates a new set or opens an existing one.
%% @end
%%--------------------------------------------------------------------
-spec new(Dir :: string() | binary(),
          Opts :: [new_opt() | open_opt()]) ->
                 {ok, mstore()} |
                 {error, filesize_missmatch}.

new(Dir, Opts) when is_binary(Dir) ->
    new(binary_to_list(Dir), Opts);
new(Dir, Opts) when is_list(Dir) , is_list(Opts) ->
    {file_size, FileSize} = proplists:lookup(file_size, Opts),
    MStore = apply_opts(#mstore{dir=Dir, size=FileSize}, Opts),
    FullIndexRead = proplists:get_bool(preload_index, Opts),
    case open_mfile(Dir, FullIndexRead) of
        {ok, F, _Metrics} when F =/= FileSize ->
            {error, filesize_missmatch};
        {ok, _F, Metrics} ->
            {ok, MStore#mstore{metrics=Metrics}};
        {error, invalid_file} = E ->
            E;
        E ->
            case file:make_dir(Dir) of
                ok ->
                    IdxFile = filename:join([Dir, "mstore"]),
                    ok = file:write_file(IdxFile, index_header(MStore)),
                    {ok, MStore#mstore{metrics=btrie:new()}};
                E1 ->
                    io:format("mstore creation errror: ~p -> ~p~n", [E, E1]),
                    E
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Opens an existing set.
%% @end
%%--------------------------------------------------------------------
-spec open(Dir :: string()) ->
                  {ok, mstore()} | {error, enoent | not_found | invalid_file}.
open(Dir) ->
    open(Dir, []).

-spec open(Dir :: string(), [open_opt()]) ->
                  {ok, mstore()} | {error, enoent | not_found | invalid_file}.
open(Dir, Opts) ->
    FullIndexRead = proplists:get_bool(preload_index, Opts),
    case open_mfile(Dir, FullIndexRead) of
        {ok, FileSize, Metrics} ->
            MStore = apply_opts(#mstore{dir=Dir, metrics=Metrics,
                                        size = FileSize}, Opts),
            {ok, MStore#mstore{}};
        Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Closes a metric set.
%% @end
%%--------------------------------------------------------------------
-spec close(mstore()) -> ok.

close(#mstore{files=Files}) ->
    [mfile:close(S) || {_, S} <- Files],
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Clones a mstore, removing currently open files form it.
%% @end
%%--------------------------------------------------------------------
-spec clone(mstore()) -> mstore().

clone(MStore = #mstore{files=[]}) ->
    MStore;
clone(MStore) ->
    MStore#mstore{files=[]}.

%%--------------------------------------------------------------------
%% @doc
%% Recursively deletes a metric set.
%% @end
%%--------------------------------------------------------------------
-spec delete(mstore()) -> ok | {error, atom()}.

delete(MStore = #mstore{dir=Dir}) ->
    close(MStore),
    case file:list_dir(Dir) of
        {ok, Files} ->
            Files1 = [filename:join([Dir, File]) || File <- Files],
            [file:delete(F) || F <- Files1],
            file:del_dir(Dir);
        {error, enoent} ->
            ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% Deletes entries before *Before*, not exact since it's rounded to
%% the next file.
%% @end
%%--------------------------------------------------------------------

-spec delete(mstore(), pos_integer()) -> {ok, mstore()}.
delete(MStore = #mstore{size = S, dir = Dir, files = Files}, Before) ->
    Before1 = ((Before div S) - 1) * S,
    Chunks = chunks(Dir, ".mstore"),
    Chunks1 = lists:takewhile(fun(C) ->
                                      C < Before1
                              end, Chunks),
    case Chunks1 of
        [] ->
            {ok, MStore};
        _ ->
            [mfile:close(F) || {_, F} <- Files],
            [begin
                 F = filename:join([Dir , integer_to_list(C)]),
                 file:delete(F ++ ".mstore"),
                 file:delete(F ++ ".bitmap"),
                 file:delete(F ++ ".idx")
             end || C <- Chunks1],
            reindex(MStore#mstore{files = []})
    end.

%%--------------------------------------------------------------------
%% @doc
%% Reads data from a set.
%% @end
%%--------------------------------------------------------------------
-spec get(
        mstore(),
        binary(),
        non_neg_integer(),
        pos_integer(), [read_opt()]) ->
                 {'error',atom()} |
                 {ok, binary()}.

get(#mstore{size=S, files=FS, dir=Dir},
    Metric, Time, Count, Opts) when
      is_binary(Metric),
      is_integer(Time), Time >= 0,
      is_integer(Count), Count > 0 ->
    ?DT_READ_ENTRY(Metric, Time, Count),
    Parts = make_splits(Time, Count, S),
    R = do_get(S, FS, Dir, Metric, Parts, <<>>, Opts),
    ?DT_READ_RETURN,
    R.

-spec get(
        mstore(),
        binary(),
        non_neg_integer(),
        pos_integer()) ->
                 {'error',atom()} |
                 {ok, binary()}.

get(MStore, Metric, Time, Count)  ->
    get(MStore, Metric, Time, Count, []).

%%--------------------------------------------------------------------
%% @doc
%% Reads the bitmap for a given time slot. Time must not be allinged
%% @end
%%--------------------------------------------------------------------

-spec bitmap(
        mstore(),
        binary(),
        non_neg_integer()) ->
                    {'error',atom()} |
                    {ok, bitmap:bitmap()}.

bitmap(#mstore{size=S, files=FS, dir=Dir},
       Metric, Time) when
      is_binary(Metric),
      is_integer(Time), Time >= 0 ->
    R = do_get_bitmap(S, FS, Dir, Metric, Time),
    R.

%%--------------------------------------------------------------------
%% @doc
%% Folds over all the metrics in a store. Please be aware that the
%% data for a single metric is not guaranteed to be delivered at a time
%% it is split on non set values and file boundaries!
%% @end
%%--------------------------------------------------------------------

-spec fold(mstore(), FoldFun :: mfile:fold_fun(), AccIn :: any()) ->
                  AccOut :: any().
fold(MStore, FoldFun, Acc) ->
    fold(MStore, FoldFun, infinity, Acc).

%%--------------------------------------------------------------------
%% @doc
%% Same as {@link: fold/3} but data is chunked at a maximum size of
%% *ChunkSize*.
%% @end
%%--------------------------------------------------------------------

-spec fold(mstore(), FoldFun :: mfile:fold_fun(),
           ChunkSize :: pos_integer() | infinity,
           AccIn :: any()) ->
                  AccOut :: any().
fold(#mstore{dir=Dir}, Fun, Chunk, Acc) ->
    serialize_dir(Dir, Fun, Chunk, Acc).

%%--------------------------------------------------------------------
%% @doc
%% Writes data into a mstore.
%% @end
%%--------------------------------------------------------------------
-spec put(mstore(), binary(), non_neg_integer(),
          integer() | [integer()] | binary()) ->
                 mstore().

put(MStore, Metric, Time, [V0 | _] = Values)
  when is_integer(V0) ->
    put(MStore, Metric, Time, mmath_bin:from_list(Values));

put(MStore, Metric, Time, V) when is_integer(V) ->
    put(MStore, Metric, Time, mmath_bin:from_list([V]));

put(MStore = #mstore{metrics = delayed}, Metric, Time, Value) ->
    {ok, MStore1} = load_index(MStore),
    put(MStore1, Metric, Time, Value);

put(MStore = #mstore{size=S, files=CurFiles, metrics=Ms},
    Metric, Time, Value)
  when is_binary(Value),
       is_integer(Time), Time >= 0,
       is_binary(Metric) ->
    ?DT_WRITE_ENTRY(Metric, Time, mmath_bin:length(Value)),
    Count = mmath_bin:length(Value),
    Parts = make_splits(Time, Count, S),
    Parts1 = [{B, trunc(C * ?DATA_SIZE)} || {B, C} <- Parts],
    MStore1 = case btrie:is_key(Metric, Ms) of
                  true ->
                      MStore;
                  false ->
                      MStorex = MStore#mstore{metrics=btrie:store(Metric, Ms)},
                      FileName = filename:join([MStorex#mstore.dir, "mstore"]),
                      ok = file:write_file(FileName,
                                           <<(byte_size(Metric)):16/integer,
                                             Metric/binary>>,
                                           [read, append]),
                      MStorex
              end,
    CurFiles1 = do_put(MStore1, Metric, Parts1, Value, CurFiles),
    ?DT_WRITE_RETURN,
    limit_files(MStore1#mstore{files = CurFiles1}).

%%--------------------------------------------------------------------
%% @doc
%% Returns a set of all metrics in the store.
%% @end
%%--------------------------------------------------------------------

-spec metrics(mstore()) -> {btrie:btrie(), mstore()}.

metrics(MStore = #mstore{metrics=delayed}) ->
    {ok, MStore1} = load_index(MStore),
    metrics(MStore1);
metrics(MStore = #mstore{metrics=M}) ->
    {M, MStore}.

%%--------------------------------------------------------------------
%% @doc
%% Returns the chunk in the mstore, each metric row in a file is a
%% chunk. So the total is the sum of the metrics in each file.
%% @end
%%--------------------------------------------------------------------

-spec count(mstore() | string()) -> non_neg_integer().
count(#mstore{dir=Dir}) ->
    count(Dir);
count(Dir) when is_list(Dir) ->
    Names = list_mfiles(Dir),
    lists:foldl(fun(N, Cnt) ->
                        Cnt + mfile:count(N)
                end, 0, Names).

%%--------------------------------------------------------------------
%% @doc
%% Returns the File Size of a store
%% @end
%%--------------------------------------------------------------------
-spec file_size(mstore()) -> non_neg_integer().
file_size(#mstore{size = FileSize}) ->
    FileSize.

%%--------------------------------------------------------------------
%% @doc
%% Utility function to split a continous data streams in chunks for a
%% given time range size.
%%
%% * Time - the offset of the first value.
%% * Count - the total number of values.
%% * Size - The size of the chunking.
%% @end
%%--------------------------------------------------------------------
-spec make_splits(Time :: non_neg_integer(),
                  Count :: pos_integer(),
                  Size :: pos_integer()) ->
                         [{StartTime :: non_neg_integer(),
                           ChunkSize :: pos_integer()}].

make_splits(Time, Count, Size) ->
    make_splits(Time, Count, Size, []).

-spec reindex(mstore()) -> {ok, mstore()}.

reindex(MStore = #mstore{dir = Dir}) ->
    IdxFileOld = filename:join([Dir, "mstore"]),
    IdxFileNew = filename:join([Dir, "mstore.new"]),

    Files = list_mfiles(Dir),
    {ok, IO} = file:open(IdxFileNew, [write | ?OPTS]),
    ok = file:write(IO, index_header(MStore)),
    Metrics = lists:foldl(fun (F, Set) ->
                                  reindex_chunk(IO, F, Set)
                          end, btrie:new(), Files),
    ok = file:close(IO),
    NewIndex = << <<(byte_size(Metric)):16/integer, Metric/binary>>
                  || Metric <- btrie:fetch_keys(Metrics) >>,
    ok = file:write_file(IdxFileNew, <<(index_header(MStore))/binary,
                                       NewIndex/binary>>),
    ok = file:delete(IdxFileOld),
    ok = file:rename(IdxFileNew, IdxFileOld),
    {ok, MStore#mstore{metrics = Metrics}}.

%%====================================================================
%% Private functions.
%%====================================================================

apply_opts(MStore, []) ->
    MStore;
apply_opts(MStore, [{max_files, N} | R]) when is_integer(N), N >= 0 ->
    apply_opts(MStore#mstore{max_files = N}, R);
apply_opts(MStore, [_ | R]) ->
    apply_opts(MStore, R).

load_index(MStore = #mstore{metrics = delayed, dir = Dir}) ->
    case open_mfile(Dir, true) of
        {ok, FileSize, Metrics} ->
            {ok, MStore#mstore{size = FileSize, metrics = Metrics}};
        E ->
            E
    end.

chunks(Dir, Ext) ->
    FileName = filename:join([Dir, "*" ++ Ext]),
    lists:sort([list_to_integer(filename:rootname(filename:basename(F))) ||
                   F <- filelib:wildcard(FileName)]).

reindex_chunk(IO, File, Set) ->
    mfile:fold_idx(fun({entry, M}, Acc) ->
                           ok = file:write(IO, <<(byte_size(M)):16/integer, M/binary>>),
                           {ok, btrie:store(M, Acc)};
                      (_, Acc) ->
                           {ok, Acc}
                   end, Set, File).

index_header(#mstore{size=FileSize}) ->
    <<?VERSION:16/?SIZE_TYPE, FileSize:64/?SIZE_TYPE>>.

make_splits(_Time, 0, _Size, Acc) ->
    lists:reverse(Acc);

make_splits(Time, Count, Size, Acc) ->
    Base = (Time div Size)*Size,
    case Time - Base of
        D when (D + Count) < Size ->
            lists:reverse([{Time, Count} | Acc]);
        D ->
            Inc = Size-D,
            make_splits(Time + Inc, Count - Inc, Size, [{Time, Inc} | Acc])
    end.



open_mfile(Dir, FullRead) ->
    F = filename:join([Dir, "mstore"]),
    Chunk = 4*1024,
    case file:open(F, [read | ?OPTS]) of
        {ok, IO} ->
            case file:read(IO, Chunk) of
                {ok, <<2:16/?SIZE_TYPE, FileSize:64/?SIZE_TYPE, R/binary>>} ->
                    Set = maybe_read_index(IO, Chunk, R, FullRead),
                    {ok, FileSize, Set};
                {ok, <<3:16/?SIZE_TYPE, FileSize:64/?SIZE_TYPE,
                       _:64/?SIZE_TYPE, R/binary>>} ->
                    Set = maybe_read_index(IO, Chunk, R, FullRead),
                    {ok, FileSize, Set};
                {ok, <<?VERSION:16/?SIZE_TYPE,
                       FileSize:64/?SIZE_TYPE, R/binary>>} ->
                    Set = maybe_read_index(IO, Chunk, R, FullRead),
                    {ok, FileSize, Set};
                {ok, _} ->
                    file:close(IO),
                    {error, invalid_file};
                _ ->
                    file:close(IO),
                    {error, invalid_file}
            end;
        E ->
            E
    end.

maybe_read_index(IO, Chunk, R, true) ->
    do_fold_idx(IO, Chunk,
                fun({entry, M}, Acc) ->
                        btrie:store(M, Acc)
                end, btrie:new(), R);
maybe_read_index(IO, _Chunk, _R, false) ->
    file:close(IO),
    delayed.

do_put(_, _, [], <<>>, Files) ->
    Files;

do_put(MStore = #mstore{size=S}, Metric,
       [{Time, Size} | R], InData,
       [{FileBase, F} | FileRest]) when
      ((Time div S)*S) =:= FileBase ->
    <<Data:Size/binary, DataRest/binary>> = InData,
    {ok, F1} = mfile:write(F, Metric, Time, Data),
    do_put(MStore, Metric, R, DataRest, [{FileBase, F1} | FileRest]);

do_put(MStore = #mstore{size=S}, Metric,
       [{Time, Size} | R], InData,
       [First, {FileBase, F}]) when
      ((Time div S)*S) =:= FileBase ->
    <<Data:Size/binary, DataRest/binary>> = InData,
    {ok, F1} = mfile:write(F, Metric, Time, Data),
    do_put(MStore, Metric, R, DataRest, [{FileBase, F1}, First]);

do_put(MStore = #mstore{size=S, dir=D}, Metric,
       [{Time, Size} | R], InData,
       [First, {_Other, F}]) ->
    <<Data:Size/binary, DataRest/binary>> = InData,
    FileBase = (Time div S)*S,
    mfile:close(F),
    Base = filename:join([D, integer_to_list(FileBase)]),
    {ok, F1} = mfile:open(Base, [{offset, FileBase},
                                 {file_size, S},
                                 {mode, write}]),
    {ok, F2} = mfile:write(F1, Metric, Time, Data),
    do_put(MStore, Metric, R, DataRest, [{FileBase, F2}, First]);

do_put(MStore = #mstore{size=S, dir=D}, Metric,
       [{Time, Size} | R], InData,
       Files) when length(Files) =< 2 ->
    <<Data:Size/binary, DataRest/binary>> = InData,
    FileBase = (Time div S)*S,
    Base = filename:join([D, integer_to_list(FileBase)]),
    {ok, F1} = mfile:open(Base, [{offset, FileBase},
                                 {file_size, S},
                                 {mode, write}]),
    {ok, F2} = mfile:write(F1, Metric, Time, Data),
    do_put(MStore, Metric, R, DataRest, [{FileBase, F2} | Files]).

limit_files(MStore = #mstore{max_files = MaxFiles,
                             files = CurFiles = [First, {_FileBase, F} | R]})
  when length(CurFiles) > MaxFiles ->
    mfile:close(F),
    limit_files(MStore#mstore{files = [First | R]});
limit_files(MStore = #mstore{max_files = MaxFiles,
                             files = CurFiles = [{_FileBase, F} | R]})
  when length(CurFiles) > MaxFiles ->
    mfile:close(F),
    limit_files(MStore#mstore{files = R});
limit_files(MStore) ->
    MStore.

do_get_bitmap(S, [{FileBase, F} | _],
              _Dir, Metric, Time)
  when ((Time div S) * S) =:= FileBase ->
    case mfile:bitmap(F, Metric) of
        {ok, B, _F1} ->
            {ok, B};
        E ->
            E
    end;
do_get_bitmap(S, [_, {FileBase, F}],
              _Dir, Metric, Time)
  when ((Time div S) * S) =:= FileBase ->
    case mfile:bitmap(F, Metric) of
        {ok, B, _F1} ->
            {ok, B};
        E ->
            E
    end;
do_get_bitmap(S, _Files, Dir, Metric, Time) ->
    Base = ((Time div S) * S),
    File = filename:join([Dir, integer_to_list(Base)]),
    case mfile:open(File) of
        {ok, F} ->
            case mfile:bitmap(F, Metric) of
                {ok, B, F1} ->
                    mfile:close(F1),
                    {ok, B};
                E ->
                    mfile:close(F),
                    E
            end;
        {error, enoent} ->
            {error, not_found}
    end.

do_get(_, _, _, _, [], Acc, _Opts) ->
    {ok, Acc};

do_get(S, [{FileBase, F} | _] = FS,
       Dir, Metric, [{Time, Count} | R], Acc, Opts)
  when ((Time div S)*S) =:= FileBase ->
    case mfile:read(F, Metric, Time, Count) of
        {ok, D} ->
            Acc1 = <<Acc/binary, D/binary>>,
            Acc2 = case mmath_bin:length(D) of
                       L when L < Count ->
                           Missing = Count - L,
                           <<Acc1/binary, (mmath_bin:empty(Missing))/binary>>;
                       _ ->
                           Acc1
                   end,
            do_get(S, FS, Dir, Metric, R, Acc2, Opts);
        {error,not_found} ->
            Acc1 = <<Acc/binary, (mmath_bin:empty(Count))/binary>>,
            do_get(S, FS, Dir, Metric, R, Acc1, Opts);
        eof ->
            Acc1 = <<Acc/binary, (mmath_bin:empty(Count))/binary>>,
            do_get(S, FS, Dir, Metric, R, Acc1, Opts);
        E ->
            E
    end;

do_get(S,
       [_, {FileBase, F}] = FS,
       Dir, Metric, [{Time, Count} | R], Acc, Opts)
  when ((Time div S)*S) =:= FileBase ->
    case mfile:read(F, Metric, Time, Count) of
        {ok, D} ->
            Acc1 = <<Acc/binary, D/binary>>,
            Acc2 = case mmath_bin:length(D) of
                       L when L < Count ->
                           Missing = Count - L,
                           <<Acc1/binary, (mmath_bin:empty(Missing))/binary>>;
                       _ ->
                           Acc1
                   end,
            do_get(S, FS, Dir, Metric, R, Acc2, Opts);
        {error,not_found} ->
            Acc1 = <<Acc/binary, (mmath_bin:empty(Count))/binary>>,
            do_get(S, FS, Dir, Metric, R, Acc1, Opts);
        eof ->
            Acc1 = <<Acc/binary, (mmath_bin:empty(Count))/binary>>,
            do_get(S, FS, Dir, Metric, R, Acc1, Opts);
        E ->
            E
    end;

do_get(S, FS, Dir, Metric, [{Time, Count} | R], Acc, Opts) ->
    FileBase = (Time div S)*S,
    Base = filename:join([Dir, integer_to_list(FileBase)]),
    Result = case proplists:get_bool(one_off, Opts) of
                 true ->
                     Base = filename:join([Dir, integer_to_list(FileBase)]),
                     mfile:one_off_read(Base, Metric, Time, Count);
                 false ->
                     {ok, F} = mfile:open(Base, [{offset, FileBase},
                                                 {file_size, S}]),
                     ReadRes = mfile:read(F, Metric, Time, Count),
                     mfile:close(F),
                     ReadRes
             end,
    case Result of
        {ok, D} ->
            Acc1 = <<Acc/binary, D/binary>>,
            Acc2 = case mmath_bin:length(D) of
                       L when L < Count ->
                           Missing = Count - L,
                           <<Acc1/binary, (mmath_bin:empty(Missing))/binary>>;
                       _ ->
                           Acc1
                   end,
            do_get(S, FS, Dir, Metric, R, Acc2, Opts);
        {error, not_found} ->
            Acc1 = <<Acc/binary, (mmath_bin:empty(Count))/binary>>,
            do_get(S, FS, Dir, Metric, R, Acc1, Opts);
        eof ->
            Acc1 = <<Acc/binary, (mmath_bin:empty(Count))/binary>>,
            do_get(S, FS, Dir, Metric, R, Acc1, Opts);
        E ->
            E
    end.


serialize_dir(Dir, Fun, Chunk, Acc) ->
    Idxs = list_mfiles(Dir),
    lists:foldl(fun(I, AccIn) ->
                        mfile:fold(I, Fun, Chunk, AccIn)
                end, Acc, Idxs).

do_fold_idx(IO, Chunk, Fun, AccIn, <<_L:16/integer, M:_L/binary, R/binary>>) ->
    Acc = Fun({entry, M}, AccIn),
    do_fold_idx(IO, Chunk, Fun, Acc, R);
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

list_mfiles(Dir) ->
    Fs = filelib:wildcard(filename:join([Dir, "*.idx"])),
    [filename:rootname(F, ".idx") || F <- Fs].
