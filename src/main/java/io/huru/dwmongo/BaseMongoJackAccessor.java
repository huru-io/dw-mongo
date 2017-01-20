package io.huru.dwmongo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.mongojack.DBCursor;
import org.mongojack.DBProjection.ProjectionBuilder;
import org.mongojack.DBSort;
import org.mongojack.JacksonDBCollection;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import com.mongodb.operation.OrderBy;

public class BaseMongoJackAccessor<T> {

	protected final MongoDBBundle mongo;
	private final Class<T> t;
	private final String collectionName;
	
	public BaseMongoJackAccessor(MongoDBBundle mongo, Class<T> t, String collectionName){
		this.mongo = mongo;
		this.t = t;
		this.collectionName = collectionName;
	}

	protected JacksonDBCollection<T,String> getCollection(String name) {
		return JacksonDBCollection.wrap(mongo.getCollection(name), t, String.class);
	}
	
	protected JacksonDBCollection<T,String> getCollection() {
		return getCollection(collectionName);
	}
	
	protected T loadBean(String key){
		return getCollection().findOneById(key);
	}
	
	protected T persistBean(T t){
		return getCollection().save(t).getSavedObject();
	}
	
	protected void remove(String id){
		getCollection().removeById(id);
	}

	protected List<T> loadPaginatedList(int startIndex, int count, Optional<OrderBy> sortOrder, Optional<String> sortBy, DBObject query, ProjectionBuilder projection) {
		DBCursor<T> cursor = getCollection().
					find(query, projection).
					limit(count).
					skip(startIndex);
		
		if (sortOrder.isPresent() && sortBy.isPresent()){
			cursor.sort(sortOrder.get() == OrderBy.ASC ? DBSort.asc(sortBy.get()): DBSort.desc(sortBy.get()));
		}
		
		return cursor.toArray();
	}

	protected List<T> loadPaginatedList(int startIndex, int count, Optional<OrderBy> sortOrder, Optional<String> sortBy, DBObject query) {
		return loadPaginatedList(startIndex, count, sortOrder, sortBy, query, null);
	}

	protected int getCount(DBObject query) {
		int count = getCollection().find(query).count();
		return count;
	}

	protected DBObject mapFieldToCaseInsensitive(String field, Object value){
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("$regex", String.format("^%s$",value));
		map.put("$options", "i");
		return QueryBuilder.start(field).is(new BasicDBObject(map)).get();
	}

	protected DBObject ascending(String fieldName) {
		return QueryBuilder.start(fieldName).is(1).get();
	}

	protected DBObject text(String ... fieldNames ) {
		DBObject dbObject = QueryBuilder.start().get();
		for (String fieldName : fieldNames) {
			dbObject.put(fieldName, "text");
		}
		return dbObject;
	}

	protected DBObject unique() {
		return QueryBuilder.start("unique").is(1).get();
	}
	
	protected Optional<T> loadBean(DBObject query) {
		List<T> list = getCollection().find(query).toArray();
		if (list.size() > 1){
			throw new IllegalArgumentException("Expected query that would return a single value but found " + list.size() +". Query is " + query);
		}
		return list.isEmpty() ? Optional.empty() : Optional.of(list.get(0));
	}

	
}