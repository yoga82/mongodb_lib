#!/usr/bin/env python
# -*- encoding: utf8 -*-
import pymongo
import sys
from bson import code

map_js = code.Code('''function() {
	var id_list = this.content.match(/@(\w+)[: ]/gi);

	if (id_list && id_list.length > 0) {
		var ids_object = new Object();
		for (var i = 0; i < id_list.length; i++){
			var id = id_list[i].replace("@", "");
			if (ids_object[id]) {
				ids_object[id] += 1;
			}
			else {
				ids_object[id] = 1;
			}
		}
		emit (this.writer, ids_object);
		//emit (this.writer, {'id_object': ids_object});
	}
}
''')


reduce_js = code.Code('''function(writer, id_object_lists) {
	//print("babo");
	var ids_object = new Object();
	for (var i = 0; i < id_object_lists.length; i++) {
		id_object = id_object_lists[i];
		//id_object = id_object_lists[i].id_object;

		for (var id in id_object) {
			if (ids_object[id]) {
				ids_object[id] += 1;
			}
			else {
				ids_object[id] = 1;
			}
		}
	}
	return ids_object;
	//return {'id_object' : ids_object};
}
''')


connection = pymongo.Connection('127.0.0.1', 10000)
db = connection.twitter
collection = db.twit

print collection.map_reduce(map_js, reduce_js, 'relation', merge_out=True, full_response=True, verbose=True)
