# MongoEngine Migration Guide

## Overview

This document describes the migration from PyMongo to MongoEngine in the NoSQL agent implementation. MongoEngine is an Object-Document Mapper (ODM) that provides a more Pythonic interface for working with MongoDB.

## What Changed

### 1. Dependencies
- **Removed**: `pymongo>=4.6.0`
- **Added**: `mongoengine>=0.29.0`

### 2. Document Models
The migration introduces proper MongoEngine document models for all collections:

#### Core Models
- `Movie` - Main movie document with embedded documents for IMDB, Tomatoes, and Awards
- `Comment` - User comments with reference to movies
- `User` - User accounts
- `Session` - User sessions
- `Theater` - Theater locations with geospatial data

#### Embedded Documents
- `ImdbInfo` - IMDB ratings and metadata
- `TomatoesInfo` - Rotten Tomatoes ratings (viewer and critic)
- `Awards` - Award information
- `Location` - Theater location with address and geospatial data

### 3. Connection Management
- **Before**: Direct PyMongo client management
- **After**: MongoEngine connection handling with automatic cleanup

### 4. Query Interface
- **Before**: Raw MongoDB queries with PyMongo
- **After**: MongoEngine ORM-style queries with fallback to aggregation pipelines

## Benefits of MongoEngine

### 1. Type Safety
```python
# Before (PyMongo)
movie = collection.find_one({"title": "Inception"})
rating = movie.get("imdb", {}).get("rating", 0)

# After (MongoEngine)
movie = Movie.objects.filter(title="Inception").first()
rating = movie.imdb.rating if movie.imdb else 0
```

### 2. Schema Validation
```python
class Movie(Document):
    title = StringField(required=True, max_length=200)
    year = IntField()
    genres = ListField(StringField(max_length=50))
    imdb = EmbeddedDocumentField(ImdbInfo)
```

### 3. Query Building
```python
# Complex queries are more readable
high_rated_action = Movie.objects.filter(
    genres__in=['Action'],
    imdb__rating__gte=7,
    year__gte=2010
).order_by('-imdb__rating').limit(10)
```

### 4. Relationship Handling
```python
# References between documents
class Comment(Document):
    movie_id = ReferenceField(Movie)
    
# Easy traversal
comment = Comment.objects.first()
movie_title = comment.movie_id.title
```

## Migration Details

### File Changes

1. **`my_agent/utils/nosql_agent.py`**
   - Replaced PyMongo imports with MongoEngine
   - Added document model definitions
   - Updated connection logic
   - Modified query execution to use MongoEngine ORM

2. **`my_agent/requirements.txt`**
   - Added `mongoengine>=0.29.0`
   - Removed `pymongo` dependency

3. **`requirements.txt`**
   - Updated main requirements to use MongoEngine

4. **`NOSQL_AGENT_README.md`**
   - Updated installation instructions

### Backward Compatibility

The migration maintains backward compatibility by:
- Supporting both find queries and aggregation pipelines
- Converting MongoEngine documents to JSON-serializable format
- Maintaining the same API interface for the `NoSQLQueryExecutor` class

## Testing the Migration

### 1. Install Dependencies
```bash
pip install mongoengine>=0.29.0
```

### 2. Run Migration Test
```bash
python test_mongoengine_migration.py
```

### 3. Test Interactive Mode
```bash
python my_agent/utils/nosql_agent.py
```

## Usage Examples

### Basic Queries
```python
from my_agent.utils.nosql_agent import NoSQLQueryExecutor

agent = NoSQLQueryExecutor()

# Natural language query
result = agent.generate_and_execute_query("Show movies from 2020")
```

### Direct MongoEngine Usage
```python
from my_agent.utils.nosql_agent import Movie, Comment

# Find movies
movies_2020 = Movie.objects.filter(year=2020)
action_movies = Movie.objects.filter(genres__in=['Action'])

# Complex queries
high_rated = Movie.objects.filter(
    imdb__rating__gte=8,
    awards__wins__gte=1
).order_by('-imdb__rating')

# Aggregations
from mongoengine.queryset.visitor import Q
pipeline = [
    {"$match": {"genres": "Action"}},
    {"$group": {"_id": "$year", "count": {"$sum": 1}}},
    {"$sort": {"_id": -1}}
]
results = Movie.objects.aggregate(pipeline)
```

## Performance Considerations

### 1. Query Optimization
- MongoEngine provides query optimization through field indexing
- Use `only()` and `exclude()` for field projection
- Leverage `select_related()` for reference field optimization

### 2. Memory Usage
- MongoEngine documents are more memory-efficient than raw dictionaries
- Automatic cleanup of connections reduces memory leaks

### 3. Connection Pooling
- MongoEngine handles connection pooling automatically
- No manual connection management required

## Troubleshooting

### Common Issues

1. **Import Errors**
   ```bash
   pip install mongoengine>=0.29.0
   ```

2. **Connection Issues**
   - Verify MongoDB connection string format
   - Check network connectivity
   - Ensure database exists

3. **Schema Mismatches**
   - MongoEngine models may not match existing data exactly
   - Use flexible field types for compatibility

### Debug Mode
```python
import mongoengine
mongoengine.connect(db='sample_mflix', host='your_connection_string')
# Enable debug mode
mongoengine.connection.get_db().command('profile', 2)
```

## Future Enhancements

1. **Validation Rules**
   - Add custom validation methods to document models
   - Implement business logic validation

2. **Indexing**
   - Define database indexes for performance optimization
   - Add text search indexes

3. **Caching**
   - Implement query result caching
   - Add Redis integration for session management

## References

- [MongoEngine Documentation](https://mongoengine-odm.readthedocs.io/)
- [MongoEngine PyPI](https://pypi.org/project/mongoengine/)
- [MongoDB Atlas Connection Guide](https://docs.atlas.mongodb.com/connect-to-cluster/)

## Migration Checklist

- [x] Update dependencies
- [x] Create document models
- [x] Update connection logic
- [x] Modify query execution
- [x] Update documentation
- [x] Create test scripts
- [x] Verify backward compatibility
- [x] Test with sample data
- [x] Update requirements files
- [x] Create migration guide

The migration is complete and ready for production use! 