import pandas as pd
import sqlite3
import json
from airflow.decorators import task

@task()
def join_tracks(items :list):
    tracks_data,albums_data,media_types_data,genres_data,artists_data = items
    tracks_data = pd.DataFrame(tracks_data)
    albums_data = pd.DataFrame(albums_data)
    media_types_data = pd.DataFrame(media_types_data)
    genres_data = pd.DataFrame(genres_data)
    artists_data = pd.DataFrame(artists_data)
    df = tracks_data.merge(albums_data, on='AlbumId', how='left').merge(media_types_data, on='MediaTypeId', how='left').merge(genres_data,on='GenreId',how='left').merge(artists_data,on='ArtistId',how='left').drop(['AlbumId','MediaTypeId','GenreId','ArtistId'],axis=1)
    df.columns = ['TrackId', 'Track', 'Composer', 'Milliseconds', 'Bytes', 
                         'UnitPrice','Title', 'MediaTypes', 'Genre', 'Artist']
    df = df.to_json(orient="records")
    df = json.loads(df)

    return {"chinook_tracks": df}

@task()
def join_playlist_tracks(items :list):
    playlist_tracks_data,playlists_data,transformed_chinook_tracks = items
    playlist_tracks_data = pd.DataFrame(playlist_tracks_data)
    playlists_data = pd.DataFrame(playlists_data)
    transformed_chinook_tracks = pd.DataFrame(transformed_chinook_tracks["chinook_tracks"])

    df = playlist_tracks_data.merge(playlists_data, on='PlaylistId', how='left').merge(transformed_chinook_tracks, on='TrackId', how='left')
    df = df.to_json(orient="records")
    df = json.loads(df)

    return {"chinook_playlist_tracks": df}

@task()
def join_invoices(items :list):
    invoices_data,customers_data,employees_data = items
    invoices_data = pd.DataFrame(invoices_data)
    employees_data = pd.DataFrame(employees_data)
    customers_data = pd.DataFrame(customers_data)

    df = invoices_data.merge(customers_data, on='CustomerId', how='left').merge(employees_data, left_on='SupportRepId', right_on='EmployeeId')
    df = df.to_json(orient="records")
    df = json.loads(df)

    return {"chinook_invoices": df}

@task()
def join_invoice_items(items :list):
    invoice_items_data, transformed_invoice_data, transformed_chinook_tracks = items
    invoice_items_data = pd.DataFrame(invoice_items_data)
    transformed_invoice_data = pd.DataFrame(transformed_invoice_data["chinook_invoices"])
    transformed_chinook_tracks = pd.DataFrame(transformed_chinook_tracks["chinook_tracks"])
    df = invoice_items_data.merge(transformed_invoice_data, on='InvoiceId', how='left').merge(transformed_chinook_tracks, on='TrackId',how='left')

    df = df.to_json(orient="records")
    df = json.loads(df)

    return {"chinook_invoice_items": df}
