import io
import mimetypes
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

SCOPES = ["https://www.googleapis.com/auth/drive"]
CLIENT_SECRET_FILE = "credentials/client_secret.json"
TOKEN_FILE = "credentials/token.json"
REDIRECT_URI = "http://127.0.0.1:5000/oauth2callback"

# Thread-local няма нужда — build() е thread-safe, credentials също
_service_cache = None


def get_google_flow(state=None):
    flow = Flow.from_client_secrets_file(
        CLIENT_SECRET_FILE,
        scopes=SCOPES,
        state=state
    )
    flow.redirect_uri = REDIRECT_URI
    return flow


def save_credentials(credentials):
    os.makedirs(os.path.dirname(TOKEN_FILE), exist_ok=True)
    with open(TOKEN_FILE, "w", encoding="utf-8") as f:
        f.write(credentials.to_json())


def load_credentials():
    if not os.path.exists(TOKEN_FILE):
        return None

    creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        save_credentials(creds)

    if creds and creds.valid:
        return creds

    return None


def get_drive_service():
    """Връща Drive service. За concurrent upload всеки thread си прави свой."""
    creds = load_credentials()
    if not creds:
        raise FileNotFoundError("Google OAuth token not found. Open /google-login first.")
    return build("drive", "v3", credentials=creds)


def find_folder(service, name, parent_id):
    safe_name = name.replace("'", "\\'")
    query = (
        f"name = '{safe_name}' and "
        f"'{parent_id}' in parents and "
        f"mimeType = 'application/vnd.google-apps.folder' and "
        f"trashed = false"
    )
    result = service.files().list(
        q=query, spaces="drive", fields="files(id, name)", pageSize=20
    ).execute()
    files = result.get("files", [])
    return files[0]["id"] if files else None


def create_folder(service, name, parent_id):
    metadata = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id],
    }
    folder = service.files().create(body=metadata, fields="id,name").execute()
    return folder["id"]


def get_or_create_folder(service, name, parent_id):
    folder_id = find_folder(service, name, parent_id)
    if folder_id:
        return folder_id
    return create_folder(service, name, parent_id)


def ensure_month_structure(root_folder_id, month, year, driver_names):
    """Създава месечна папка + подпапки за шофьори + Файлове."""
    service = get_drive_service()

    month_names = {
        1: "Януари", 2: "Февруари", 3: "Март", 4: "Април",
        5: "Май", 6: "Юни", 7: "Юли", 8: "Август",
        9: "Септември", 10: "Октомври", 11: "Ноември", 12: "Декември",
    }

    month_name = f"{month_names.get(month, month)} {year}"
    month_folder_id = get_or_create_folder(service, month_name, root_folder_id)
    files_folder_id = get_or_create_folder(service, "Файлове", month_folder_id)

    driver_folder_map = {}
    for name in sorted(set(driver_names)):
        clean_name = str(name).strip()
        if not clean_name:
            continue
        folder_id = get_or_create_folder(service, clean_name, month_folder_id)
        driver_folder_map[clean_name] = folder_id

    return {
        "month_folder_id": month_folder_id,
        "files_folder_id": files_folder_id,
        "driver_folder_map": driver_folder_map,
    }


def _upload_single_file(folder_id, filename, content_bytes, mimetype):
    """Вътрешна функция за upload на 1 файл — всеки thread си прави service."""
    service = get_drive_service()
    mimetype = mimetype or mimetypes.guess_type(filename)[0] or "application/octet-stream"
    file_metadata = {"name": filename, "parents": [folder_id]}
    media = MediaIoBaseUpload(io.BytesIO(content_bytes), mimetype=mimetype, resumable=False)
    return service.files().create(body=file_metadata, media_body=media, fields="id,name").execute()


def _find_file_in_folder(service, filename, folder_id):
    """Търси файл по име в конкретна папка. Връща file_id или None."""
    safe_name = filename.replace("'", "\\'")
    query = (
        f"name = '{safe_name}' and "
        f"'{folder_id}' in parents and "
        f"mimeType != 'application/vnd.google-apps.folder' and "
        f"trashed = false"
    )
    result = service.files().list(
        q=query, spaces="drive", fields="files(id, name)", pageSize=1
    ).execute()
    files = result.get("files", [])
    return files[0]["id"] if files else None


def _upload_or_replace_single_file(folder_id, filename, content_bytes, mimetype):
    """
    Upload или replace на 1 файл — всеки thread си прави service.
    Ако файл със същото име вече съществува в папката — презаписва го (update).
    Ако няма — създава нов (create).
    """
    service = get_drive_service()
    mimetype = mimetype or mimetypes.guess_type(filename)[0] or "application/octet-stream"
    media = MediaIoBaseUpload(io.BytesIO(content_bytes), mimetype=mimetype, resumable=False)

    existing_id = _find_file_in_folder(service, filename, folder_id)

    if existing_id:
        # Update — само съдържанието, без да пипаме parents/name
        return service.files().update(
            fileId=existing_id,
            media_body=media,
            fields="id,name"
        ).execute()
    else:
        file_metadata = {"name": filename, "parents": [folder_id]}
        return service.files().create(
            body=file_metadata, media_body=media, fields="id,name"
        ).execute()


def upload_file_bytes_to_drive(folder_id, filename, content_bytes, mimetype=None):
    """Upload на 1 файл (синхронен, за обратна съвместимост)."""
    return _upload_single_file(folder_id, filename, content_bytes, mimetype)


def upload_buffer_to_drive(folder_id, filename, buffer_obj, mimetype=None):
    """Upload на 1 файл от buffer (синхронен, за обратна съвместимост)."""
    buffer_obj.seek(0)
    content_bytes = buffer_obj.read()
    return upload_file_bytes_to_drive(folder_id, filename, content_bytes, mimetype=mimetype)


def upload_files_batch(file_list, max_workers=10):
    """
    Concurrent upload на много файлове.
    file_list = [(folder_id, filename, content_bytes, mimetype), ...]
    Връща (successful_count, errors_list)
    """
    successful = 0
    errors = []

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_to_name = {}
        for folder_id, filename, content_bytes, mimetype in file_list:
            future = pool.submit(_upload_or_replace_single_file, folder_id, filename, content_bytes, mimetype)
            future_to_name[future] = filename

        for future in as_completed(future_to_name):
            fname = future_to_name[future]
            try:
                future.result()
                successful += 1
            except Exception as e:
                errors.append(f"{fname}: {str(e)}")

    return successful, errors


def get_root_folder_id():
    return os.environ.get("GDRIVE_ROOT_FOLDER_ID")
