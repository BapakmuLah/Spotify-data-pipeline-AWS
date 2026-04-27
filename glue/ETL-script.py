import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame

# Script generated for node Artist Transform
def ArtistTransform(glueContext, dfc) -> DynamicFrameCollection:
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection
    from pyspark.sql.functions import col, when, trim

    # CONVERT DYNAMIC FRAME INTO DATAFRAME
    df = dfc.select(list(dfc.keys())[0]).toDF()

    # 🔥 TAMBAHAN: EMPTY STRING → NULL
    df = df.select([when(trim(col(c)) == "", None).otherwise(col(c)).alias(c) 
                    for c in df.columns])

    # HANDLE MISSING VALUES
    df = df.fillna({"main_genre" : "Unknown Genre",
                    "genre_1" : "No Genre",
                    "genre_2" : "No Genre",
                    "genre_3" : "No Genre",
                    "genre_4" : "No Genre",
                    "genre_5" : "No Genre",
                    "genre_6" : "No Genre"
                    
    })

    # CONVERT BACK TO DYNAMICFRAME
    new_dyf = DynamicFrame.fromDF(df, glueContext, "new_dyf")

    return DynamicFrameCollection({'output' : new_dyf}, glueContext)
# Script generated for node Albums Transform
def AlbumTransform(glueContext, dfc) -> DynamicFrameCollection:
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection
    from pyspark.sql.functions import col, when, trim


    # CONVERT DYNAMIC FRAME INTO DATAFRAME
    df = dfc.select(list(dfc.keys())[0]).toDF()

    # 🔥 TAMBAHAN: EMPTY STRING → NULL
    df = df.select([when(trim(col(c)) == "", None).otherwise(col(c)).alias(c) 
                    for c in df.columns])

    # HANDLE MISSING VALUES
    df = df.fillna({"track_name" : "Unknown Track",
                    "album_name" : "No Album",
                    "release_date" : "Unknown",
                    "label" : "No Label",
                    "main_artist" : "Unknown Artist",
                    "artist_1" : "No Artist",
                    "artist_2" : "No Artist",
                    "artist_3" : "No Artist",
                    "artist_4" : "No Artist",
                    "artist_5" : "No Artist",
                    "artist_6" : "No Artist",
                    "artist_7" : "No Artist",
                    "artist_8" : "No Artist",
                    "artist_9" : "No Artist",
                    "artist_10" : "No Artist",
                    "artist_11" : "No Artist",
    })

    # CONVERT BACK TO DYNAMICFRAME
    new_dyf = DynamicFrame.fromDF(df, glueContext, "new_dyf")

    return DynamicFrameCollection({'output' : new_dyf}, glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Albums
Albums_node1775828255586 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://spotify-bucket-549710416811-ap-southeast-2-an/staging/spotify-albums_data_2023.csv"], "recurse": True}, transformation_ctx="Albums_node1775828255586")

# Script generated for node Tracks
Tracks_node1775828256615 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://spotify-bucket-549710416811-ap-southeast-2-an/staging/spotify_tracks_data_2023.csv"], "recurse": True}, transformation_ctx="Tracks_node1775828256615")

# Script generated for node Artists
Artists_node1775828256143 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://spotify-bucket-549710416811-ap-southeast-2-an/staging/spotify_artist_data_2023.csv"], "recurse": True}, transformation_ctx="Artists_node1775828256143")

# Script generated for node Drop Useless Columns (Drop Artists)
DropUselessColumnsDropArtists_node1775829536973 = DropFields.apply(frame=Albums_node1775828255586, paths=["artists"], transformation_ctx="DropUselessColumnsDropArtists_node1775829536973")

# Script generated for node Tracks Schema
TracksSchema_node1775843790926 = ApplyMapping.apply(frame=Tracks_node1775828256615, mappings=[("id", "string", "id", "string"), ("track_popularity", "string", "track_popularity", "int"), ("explicit", "string", "explicit", "boolean")], transformation_ctx="TracksSchema_node1775843790926")

# Script generated for node Artists Schema
ArtistsSchema_node1775845758368 = ApplyMapping.apply(frame=Artists_node1775828256143, mappings=[("id", "string", "id", "string"), ("name", "string", "name", "string"), ("artist_popularity", "string", "artist_popularity", "int"), ("artist_genres", "string", "artist_genres", "string"), ("followers", "string", "followers", "int"), ("genre_0", "string", "main_genre", "string"), ("genre_1", "string", "genre_1", "string"), ("genre_2", "string", "genre_2", "string"), ("genre_3", "string", "genre_3", "string"), ("genre_4", "string", "genre_4", "string"), ("genre_5", "string", "genre_5", "string"), ("genre_6", "string", "genre_6", "string")], transformation_ctx="ArtistsSchema_node1775845758368")

# Script generated for node Albums Schema
AlbumsSchema_node1775829751899 = ApplyMapping.apply(frame=DropUselessColumnsDropArtists_node1775829536973, mappings=[("track_name", "string", "track_name", "string"), ("track_id", "string", "track_id", "string"), ("track_number", "string", "track_number", "int"), ("duration_ms", "string", "duration_ms", "int"), ("album_type", "string", "album_type", "string"), ("total_tracks", "string", "total_tracks", "int"), ("album_name", "string", "album_name", "string"), ("release_date", "string", "release_date", "string"), ("label", "string", "label", "string"), ("album_popularity", "string", "album_popularity", "int"), ("album_id", "string", "album_id", "string"), ("artist_id", "string", "artist_id", "string"), ("artist_0", "string", "main_artist", "string"), ("artist_1", "string", "artist_1", "string"), ("artist_2", "string", "artist_2", "string"), ("artist_3", "string", "artist_3", "string"), ("artist_4", "string", "artist_4", "string"), ("artist_5", "string", "artist_5", "string"), ("artist_6", "string", "artist_6", "string"), ("artist_7", "string", "artist_7", "string"), ("artist_8", "string", "artist_8", "string"), ("artist_9", "string", "artist_9", "string"), ("artist_10", "string", "artist_10", "string"), ("artist_11", "string", "artist_11", "string"), ("duration_sec", "string", "duration_sec", "float")], transformation_ctx="AlbumsSchema_node1775829751899")

# Script generated for node Artist Transform
ArtistTransform_node1775845967390 = ArtistTransform(glueContext, DynamicFrameCollection({"ArtistsSchema_node1775845758368": ArtistsSchema_node1775845758368}, glueContext))

# Script generated for node Albums Transform
AlbumsTransform_node1775830982814 = AlbumTransform(glueContext, DynamicFrameCollection({"AlbumsSchema_node1775829751899": AlbumsSchema_node1775829751899}, glueContext))

# Script generated for node Artist DYF collection
ArtistDYFcollection_node1775847168437 = SelectFromCollection.apply(dfc=ArtistTransform_node1775845967390, key=list(ArtistTransform_node1775845967390.keys())[0], transformation_ctx="ArtistDYFcollection_node1775847168437")

# Script generated for node Albums DYF collection
AlbumsDYFcollection_node1775846583612 = SelectFromCollection.apply(dfc=AlbumsTransform_node1775830982814, key=list(AlbumsTransform_node1775830982814.keys())[0], transformation_ctx="AlbumsDYFcollection_node1775846583612")

# Script generated for node Join Album & Artists
AlbumsDYFcollection_node1775846583612DF = AlbumsDYFcollection_node1775846583612.toDF()
ArtistDYFcollection_node1775847168437DF = ArtistDYFcollection_node1775847168437.toDF()
JoinAlbumArtists_node1775846446824 = DynamicFrame.fromDF(AlbumsDYFcollection_node1775846583612DF.join(ArtistDYFcollection_node1775847168437DF, (AlbumsDYFcollection_node1775846583612DF['artist_id'] == ArtistDYFcollection_node1775847168437DF['id']), "left"), glueContext, "JoinAlbumArtists_node1775846446824")

# Script generated for node Join
JoinAlbumArtists_node1775846446824DF = JoinAlbumArtists_node1775846446824.toDF()
TracksSchema_node1775843790926DF = TracksSchema_node1775843790926.toDF()
Join_node1775847239324 = DynamicFrame.fromDF(JoinAlbumArtists_node1775846446824DF.join(TracksSchema_node1775843790926DF, (JoinAlbumArtists_node1775846446824DF['track_id'] == TracksSchema_node1775843790926DF['id']), "left"), glueContext, "Join_node1775847239324")

# Script generated for node Drop Fields
DropFields_node1775847714718 = DropFields.apply(frame=Join_node1775847239324, paths=["id", "`.id`"], transformation_ctx="DropFields_node1775847714718")

# Script generated for node Destination to S3
EvaluateDataQuality().process_rows(frame=DropFields_node1775847714718, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1775843411924", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
DestinationtoS3_node1775848787129 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1775847714718, connection_type="s3", format="glueparquet", connection_options={"path": "s3://spotify-bucket-549710416811-ap-southeast-2-an/datawarehouse/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="DestinationtoS3_node1775848787129")

job.commit()