#  The MovieLens 100k Dataset Analysis
To create a Python script that integrates Apache Spark and Cassandra to analyze the MovieLens 100K dataset, follow these steps. This script will include functions to parse the `u.user` file, load data into HDFS, create RDDs, convert them to DataFrames, and perform the required queries using Spark SQL and Cassandra.

## Project Overview

1. **Python Libraries**: Use PySpark for Spark operations and `cassandra-driver` for Cassandra interactions.
2. **Parse `u.user` File**: Load the user data into HDFS.
3. **Load Data into RDDs**: Create RDDs from the data files.
4. **Convert RDDs to DataFrames**: Use Spark SQL to convert RDDs to DataFrames.
5. **Write DataFrames to Cassandra**: Store the DataFrames in Cassandra tables.
6. **Read Data from Cassandra**: Load the data back into Spark DataFrames for querying.

## Script 
```
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.window import Window
```
##### List of genre names based on the u.item file structure
```
genre_names = [
    "unknown", "Action", "Adventure", "Animation", "Children's", "Comedy",
    "Crime", "Documentary", "Drama", "Fantasy", "Film-Noir", "Horror",
    "Musical", "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western"
]

def parse_rating(line):
    fields = line.split('\t')
    return Row(user_id=int(fields[0]), movie_id=int(fields[1]), rating=int(fields[2]), timestamp=int(fields[3]))

def parse_movie(line):
    fields = line.split('|')
    genres = [genre_names[i - 5] for i in range(5, 24) if fields[i] == '1']
    return Row(movie_id=int(fields[0]), title=fields[1], genres=genres)

def parse_user(line):
    fields = line.split('|')
    return Row(user_id=int(fields[0]), age=int(fields[1]), gender=fields[2], occupation=fields[3], zip_code=fields[4])
    
def write_to_cassandra(dataframe, table, keyspace):
    dataframe.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=table, keyspace=keyspace) \
        .mode("append") \
        .save()

def read_from_cassandra(table, keyspace):
    return spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=table, keyspace=keyspace) \
        .load()

if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("MovieLensAnalysis") \
        .config("spark.cassandra.connection.host", "127.0.0.1") \
        .getOrCreate()
```
##### Load the ratings data
```
    ratings_lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/irffan/ml-100k/u.data")
    ratings = ratings_lines.map(parse_rating)
    ratingsDataset = spark.createDataFrame(ratings)
```
##### Load the movies data
```
    movies_lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/irffan/ml-100k/u.item")
    movies = movies_lines.map(parse_movie)
    moviesDataset = spark.createDataFrame(movies)
```

##### Load the users data
```
    users_lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/irffan/ml-100k/u.user")
    users = users_lines.map(parse_user)
    usersDataset = spark.createDataFrame(users)
```
##### Calculate the average rating for each movie
```
    averageRatings = ratingsDataset.groupBy("movie_id").agg(F.avg("rating").alias("avg_rating"))
```
##### Join with the movie titles
```
    movieRatings = averageRatings.join(moviesDataset, "movie_id").select("movie_id", "title", "avg_rating")
```
##### Display the average rating for each movie (top 10 without sorting)
```
    print("Average rating for each movie:")
    movieRatings.show(10, truncate=False)
```
##### Calculate the average rating and count of ratings for each movie
```
    movieStats = ratingsDataset.groupBy("movie_id").agg(
        F.avg("rating").alias("avg_rating"),
        F.count("rating").alias("rating_count")
    )
```
##### Join with the movie titles
```
    movieRatingsWithCount = movieStats.join(moviesDataset, "movie_id").select("movie_id", "title", "avg_rating", "rating_count")
```
##### Identify the top ten movies with the highest average rating 
```
    topTenMovies = movieRatingsWithCount.orderBy(F.desc("avg_rating")).limit(10).select("movie_id", "title", "avg_rating", "rating_count")
```
##### Display the top ten movies with the highest rating count
```
    print("Top ten movies with the highest rating average:")
    topTenMovies.show(truncate=False)
```

##### Identify the top ten movies with the highest rating count
```
    topTenMoviesRatingCount = movieRatingsWithCount.orderBy(F.desc("rating_count")).limit(10).select("movie_id", "title", "avg_rating", "rating_count")
```
##### Display the top ten movies with the highest rating count
```
    print("Top ten movies with the highest rating count:")
    topTenMoviesRatingCount.show(truncate=False)
```
##### Filter out movies with less than 10 ratings
```
    popularMovies = movieStats.filter("rating_count > 10")
```
##### Join with the movie titles
```
    movieRatingsWithCount1 = popularMovies.join(moviesDataset, "movie_id").select("movie_id", "title", "avg_rating", "rating_count")
```
##### Identify the movie with the highest average rating
```
    topMovie = movieRatingsWithCount1.orderBy(F.desc("avg_rating")).limit(10).select("movie_id", "title", "avg_rating", "rating_count")
```

##### Display the movie with the highest average rating
```
    print("Movie with the highest average rating (with more than 10 ratings count):")
    topMovie.show(truncate=False)
```
##### Find the users who have rated at least 50 movies
```
    usersWith50Ratings = ratingsDataset.groupBy("user_id").agg(F.count("rating").alias("num_ratings")).filter("num_ratings >= 50")
```
##### Explode genres array for easier aggregation
```
    explodedMoviesDataset = moviesDataset.withColumn("genre", F.explode(F.col("genres")))
```
##### Join datasets to get user, rating, and genre information
```
    usersGenres = ratingsDataset.join(explodedMoviesDataset, "movie_id").join(usersWith50Ratings, "user_id")
```
##### Aggregate to find the count of ratings per genre for each user
```
    usersGenresCount = usersGenres.groupBy("user_id", "genre").agg(F.count("genre").alias("genre_count"))
```
##### Window function to identify favourite genre(s) for each user
```
    windowSpec = Window.partitionBy("user_id").orderBy(F.desc("genre_count"))
    favouriteGenres = usersGenresCount.withColumn("rank", F.rank().over(windowSpec)).filter(F.col("rank") == 1).drop("rank")
```
##### Join with usersDataset to get gender
```
    favouriteGenresWithGender = favouriteGenres.join(usersDataset, "user_id").select("user_id", "gender", "genre", "genre_count")
```
##### Display the users who have rated at least 50 movies, their favourite movie genres, and gender (top 10)
```
    print("Top ten users who have rated at least 50 movies, their favourite movie genres, and gender:")
    favouriteGenresWithGender.show(10, truncate=False)
```
##### Find all the users with age less than 20 years old
```
    youngUsers = usersDataset.filter(usersDataset["age"] < 20)
```
##### Display the users with age less than 20 years old (top 10)
```
    print("Top ten users with age less than 20 years old:")
    youngUsers.show(10, truncate=False)
```
##### Find all the users who have the occupation "scientist" and their age is between 30 and 40 years old
```
    scientistUsers = usersDataset.filter((usersDataset["occupation"] == "scientist") & (usersDataset["age"] >= 30) & (usersDataset["age"] <= 40))
```
##### Display the users who have the occupation "scientist" and their age is between 30 and 40 years old (top 10)
```
    print("Top ten users who have the occupation 'scientist' and their age is between 30 and 40 years old:")
    scientistUsers.show(10, truncate=False)
```
##### Write the movieRatings DataFrame into Cassandra keyspace
```
    write_to_cassandra(movieRatings, "movie_ratings", "movielens")
    write_to_cassandra(topTenMovies, "top_ten_movies_avg_rating", "movielens")
    write_to_cassandra(topTenMoviesRatingCount, "top_ten_movies_rating_count", "movielens")
    write_to_cassandra(movieRatingsWithCount1, "popular_movies", "movielens")
    write_to_cassandra(favouriteGenresWithGender, "favourite_genres", "movielens")
    write_to_cassandra(youngUsers, "young_users", "movielens")
    write_to_cassandra(scientistUsers, "scientist_users", "movielens")
    
```
##### Read the tables back from Cassandra into new DataFrames
```
    movieRatingsFromCassandra = read_from_cassandra("movie_ratings", "movielens")
    topTenMoviesFromCassandra = read_from_cassandra("top_ten_movies_avg_rating", "movielens")
    topTenMoviesRatingCountFromCassandra = read_from_cassandra("top_ten_movies_rating_count", "movielens")
    popularMoviesFromCassandra = read_from_cassandra("popular_movies", "movielens")
    favouriteGenresFromCassandra = read_from_cassandra("favourite_genres", "movielens")
    youngUsersFromCassandra = read_from_cassandra("young_users", "movielens")
    scientistUsersFromCassandra = read_from_cassandra("scientist_users", "movielens")
```
##### Display the DataFrames read back from Cassandra
```
    print("Movie ratings from Cassandra:")
    movieRatingsFromCassandra.show(10, truncate=False)
    print("Top ten movies by average rating from Cassandra:")
    topTenMoviesFromCassandra.show(10, truncate=False)
    print("Top ten movies by rating count from Cassandra:")
    topTenMoviesRatingCountFromCassandra.show(10, truncate=False)
    print("Popular movies from Cassandra:")
    popularMoviesFromCassandra.show(10, truncate=False)
    print("Favourite genres from Cassandra:")
    favouriteGenresFromCassandra.show(10, truncate=False)
    print("Young users from Cassandra:")
    youngUsersFromCassandra.show(10, truncate=False)
    print("Scientist users from Cassandra:")
    scientistUsersFromCassandra.show(10, truncate=False)
```
##### Stop the SparkSession
```
    spark.stop()
```
## Keyspace database created in Cassandra.
### Inside cqlsh
```
cqlsh> CREATE KEYSPACE IF NOT EXISTS movielens WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

cqlsh> CREATE TABLE IF NOT EXISTS movielens.movie_ratings (
    movie_id int,
    title text,
    avg_rating float,
    PRIMARY KEY (movie_id)
);

cqlsh> CREATE TABLE IF NOT EXISTS movielens.top_ten_movies_avg_rating (
    movie_id int,
    title text,
    avg_rating float,
    rating_count int,
    PRIMARY KEY (movie_id)
);

cqlsh> CREATE TABLE IF NOT EXISTS movielens.top_ten_movies_rating_count (
    movie_id int,
    title text,
    avg_rating float,
    rating_count int,
    PRIMARY KEY (movie_id)
);

cqlsh> CREATE TABLE IF NOT EXISTS movielens.popular_movies (
    movie_id int,
    title text,
    avg_rating float,
    rating_count int,
    PRIMARY KEY (movie_id)
);

cqlsh> CREATE TABLE IF NOT EXISTS movielens.favourite_genres (
    user_id int,
    gender text,
    genre text,
    genre_count int,
    PRIMARY KEY (user_id, genre)
);

cqlsh> CREATE TABLE IF NOT EXISTS movielens.young_users (
    user_id int,
    age int,
    gender text,
    occupation text,
    zip_code text,
    PRIMARY KEY (user_id)
);

cqlsh> CREATE TABLE IF NOT EXISTS movielens.scientist_users (
    user_id int,
    age int,
    gender text,
    occupation text,
    zip_code text,
    PRIMARY KEY (user_id)
);
```
## Results
#### Average Rating for Each Movie :

| movie_id | title | avg_rating |
|----------|-------|-------------|
| 26 | Brothers McMullen, The (1995) | 3.452054794520548 |
| 29 | Batman Forever (1995) | 2.6666666666666665 |
| 474 | Dr. Strangelove or: How I Learned to Stop Worrying and Love the Bomb (1963) | 4.252577319587629 |
| 964 | Month by the Lake, A (1995) | 3.3333333333333335 |
| 1677 | Sweet Nothing (1995) | 3.00 |
| 65 | What's Eating Gilbert Grape (1993) | 3.5391304347826087 |
| 191 | Amadeus (1984) | 4.163043478260869 |
| 418 | Cinderella (1950) | 3.5813953488372094 |
| 541 | Mortal Kombat (1995) |2.877551020408163 |
| 558 | Heavenly Creatures (1994) | 3.6714285714285713 |

#### Top Ten Movies with the Highest Average Ratings :

| movie_id | title | avg_rating | rating_count |
|----------|-------|-------------|--------------|
| 1189 | Prefontaine (1997) | 5.0 | 3 |
| 1467 | Saint of Fort Washington, The (1993) | 5.0 | 2 |
| 1201 | Marlene Dietrich: Shadow and Light (1996) | 5.0 | 1 |
| 1599 | Someone Else's America (1995) | 5.0 | 1 |
| 814 | Great Day in Harlem, A (1994) | 5.0 | 1 |
| 1653 | Entertaining Angels: The Dorothy Day Story (1996) | 5.0 | 1 |
| 1122 | They Made Me a Criminal (1939) | 5.0 | 1 |
| 1500 | Santa with Muscles (1996) | 5.0 | 2 |
| 1536 | Aiqing wansui (1994) | 5.0 | 1 |
| 1293 | Star Kid (1997) | 5.0 | 3 |

#### Top ten movies with the highest rating count:

|movie_id|title                        |avg_rating        |rating_count|
|--------|-----------------------------|------------------|------------|
|50      |Star Wars (1977)             |4.3584905660377355|583         |
|258     |Contact (1997)               |3.8035363457760316|509         |
|100     |Fargo (1996)                 |4.155511811023622 |508         |
|181     |Return of the Jedi (1983)    |4.007889546351085 |507         |
|294     |Liar Liar (1997)             |3.156701030927835 |485         |
|286     |English Patient, The (1996)  |3.656964656964657 |481         |
|288     |Scream (1996)                |3.4414225941422596|478         |
|1       |Toy Story (1995)             |3.8783185840707963|452         |
|300     |Air Force One (1997)         |3.6310904872389793|431         |
|121     |Independence Day (ID4) (1996)|3.438228438228438 |429         |


#### Movie with the highest average rating (with more than 10 ratings count):

|movie_id|title                                                 |avg_rating        |rating_count|
|--------|------------------------------------------------------|------------------|------------|
|408     |Close Shave, A (1995)                                 |4.491071428571429 |112         |
|318     |Schindler's List (1993)                               |4.466442953020135 |298         |
|169     |Wrong Trousers, The (1993)                            |4.466101694915254 |118         |
|483     |Casablanca (1942)                                     |4.45679012345679  |243         |
|114     |Wallace & Gromit: The Best of Aardman Animation (1996)|4.447761194029851 |67          |
|64      |Shawshank Redemption, The (1994)                      |4.445229681978798 |283         |
|603     |Rear Window (1954)                                    |4.3875598086124405|209         |
|12      |Usual Suspects, The (1995)                            |4.385767790262173 |267         |
|50      |Star Wars (1977)                                      |4.3584905660377355|583         |
|178     |12 Angry Men (1957)                                   |4.344             |125         |

#### Top ten users who have rated at least 50 movies, their favourite movie genres, and gender:

| user_id | gender | genre | genre_count |
|---------|--------|-------|-------------|
| 26 | M | Drama | 41 |
| 474 | M | Drama | 166 |
| 65 | F | Drama | 46 |
| 541 | F | Comedy | 52 |
| 222 | M | Comedy | 131 |
| 270 | F | Drama | 56 |
| 293 | M | Drama | 165 |
| 938 | F | Comedy | 40 |
| 243 | M | Drama | 59 |
| 367 | M | Horror | 31 |

#### Top ten users with age less than 20 years old:

| age | gender | occupation | user_id | zip_code |
|-----|--------|------------|---------|----------|
| 7 | M | student | 30 | 55436 |
| 19 | F | student | 36 | 93117 |
| 18 | F | student | 52 | 55105 |
| 16 | M | none | 57 | 84010 |
| 17 | M | student | 67 | 60402 |
| 19 | M | student | 68 | 22904 |
| 15 | M | student | 101 | 05146 |
| 19 | M | student | 110 | 77840 |
| 13 | M | other | 142 | 48118 |
| 15 | M | entertainment | 179 | 20755 |

#### Top ten users who have the occupation 'scientist' and their age is between 30 and 40 years old:

| age | gender | occupation | user_id | zip_code |
|-----|--------|------------|---------|----------|
| 38 | M | scientist | 40 | 27514 |
| 39 | M | scientist | 71 | 98034 |
| 39 | M | scientist | 74 | T8H1N |
| 39 | M | scientist | 107 | 60466 |
| 33 | M | scientist | 183 | 27708 |
| 33 | M | scientist | 272 | 53706 |
| 40 | M | scientist | 309 | 70802 |
| 37 | M | scientist | 337 | 10522 |
| 38 | M | scientist | 430 | 98199 |
| 31 | M | scientist | 538 | 21010 |

## Insight
1. Average rating for each movie:
This table shows the average ratings for 10 movies. The ratings range from 2.67 to 4.25, with "Dr. Strangelove" (1963) having the highest average rating of 4.25 and "Batman Forever" (1995) having the lowest at 2.67. This gives us a snapshot of how these particular movies were received by viewers.

2. Top ten movies with the highest rating average:
Interestingly, all movies in this list have a perfect 5.0 rating. However, it's crucial to note that most of these movies have very few ratings (1-3 counts). This suggests that while these movies are highly rated, the sample size is too small to draw meaningful conclusions about their overall popularity or quality.

3. Top ten movies with the highest rating count:
This list provides more reliable insights as it shows popular movies with a large number of ratings. "Star Wars" (1977) tops the list with 583 ratings and an average of 4.36, indicating both popularity and high quality. Other well-known films like "Fargo," "Return of the Jedi," and "Toy Story" also appear, showing a mix of different genres among the most-rated movies.

4. Movie with the highest average rating (with more than 10 ratings count):
This list is more meaningful as it combines high ratings with a significant number of reviews. "A Close Shave" (1995) tops the list with an average rating of 4.49 from 112 ratings. Classic films like "Casablanca" and critically acclaimed movies like "Schindler's List" also feature prominently, indicating a correlation between critical acclaim and user ratings.

5. Top ten users who have rated at least 50 movies, their favourite movie genres, and gender:
This data provides insights into user preferences. Drama and Comedy are the most common favorite genres among these active users. There's a slight male majority in this group (6 out of 10), but both genders are represented among the most active users.

6. Top ten users with age less than 20 years old:
This list shows that younger users (under 20) are predominantly male and mostly students. The youngest user is 7 years old, which raises questions about parental supervision or the accuracy of user-reported data.

7. Top ten users who have the occupation 'scientist' and their age is between 30 and 40 years old:
This group consists entirely of male scientists in their 30s, with ages ranging from 31 to 40. This could indicate a gender imbalance in this particular demographic of the user base, or in the scientific profession during the time this data was collected.

Overall, these outputs provide valuable insights into user demographics, movie preferences, and rating patterns within this dataset. They highlight popular and highly-rated movies, active user characteristics, and some interesting demographic trends among different user groups.

