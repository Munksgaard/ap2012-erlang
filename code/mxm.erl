-module(mxm).

-export([wordnum/1,count_words/2, count_tracklist_words/2]).
-import(read_mxm).
-import(mr).

wordnum(Filename) ->
    {Wordlist, Tracklist} = read_mxm:from_file(Filename),
    {ok, MR} = mr:start(100),
    mr:job(MR, fun count_words/1, fun count_tracklist_words/2, 0, Tracklist).

count_tracklist_words(X, Acc) ->
    X + Acc.

count_words(Track) ->
    {_, _, Wordcounts} = read_mxm:parse_track(Track),
    count_words(Wordcounts, 0).

count_words([], Acc) -> Acc;
count_words([{_, Count} | Rest], Acc) ->
    count_words(Rest, Acc + Count).


