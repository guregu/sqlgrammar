:- use_module(sql).
:- use_module(library(pio)).

test :-
	phrase_to_stream(sqlc_crud("widget", [
		col("widget_id", uuid, []),
		col("foo", bigint, []),
		col("bar", decimal, [null(true)]),
		col("baz", decimal(10, 2), []),
        col("bread_type",enum(["白パン", "黒パン"]),[null(true)])
	], [
		primary(["widget_id", "foo"]),
		unique(["baz"])
	]), stdout),
    !.
