from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json

CREDENTIALS_PATH = "credentials/"


def connectToDB():
    # This secure connect bundle is autogenerated when you download your SCB,
    # if yours is different update the file name below
    cloud_config = {
        "secure_connect_bundle": CREDENTIALS_PATH
        + "secure-connect-big-data-project-db.zip"
    }

    # This token JSON file is autogenerated when you download your token,
    # if yours is different update the file name below
    with open(CREDENTIALS_PATH + "big_data_project_db-token.json") as f:
        secrets = json.load(f)

    CLIENT_ID = secrets["clientId"]
    CLIENT_SECRET = secrets["secret"]

    auth_provider = PlainTextAuthProvider(CLIENT_ID, CLIENT_SECRET)
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()

    row = session.execute("select release_version from system.local").one()
    if row:
        print("Connection successful!")
        return session
    else:
        print("An error occurred.")
        return -1


connectToDB()
