import datetime as dt
import logging
import os
from typing import Iterable, Optional

import azure.functions as func
from azure.storage.blob import (
    BlobServiceClient,
    BlobClient,
    ContainerClient,
    BlobLeaseClient,
    StorageStreamDownloader,
)

# ---- 環境変数（docker-compose で注入推奨） ----
CONN_STR = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
CONTAINER_NAME = os.environ["AZURE_STORAGE_CONTAINER"]
PREFIX = os.environ.get("PROCESS_PREFIX", "")
TXT_SUFFIX = os.environ.get("TXT_PROCESS_SUFFIX", ".txt")
CSV_SUFFIX = os.environ.get("CSV_PROCESS_SUFFIX", ".csv")

# Django 側アップロード完了を待ちたい場合の「完成マーカー」案
# 例: 同名ファイルの「.done」や「_READY」メタデータを見る運用も可
READY_MARKER_SUFFIX = os.environ.get("READY_MARKER_SUFFIX", "")  # 例: ".done"


def _list_target_blobs(container: ContainerClient) -> Iterable[str]:
    for blob in container.list_blobs(name_starts_with=PREFIX):
        name = blob["name"] if isinstance(blob, dict) else blob.name
        if TXT_SUFFIX and not name.endswith(TXT_SUFFIX):
            continue
        if CSV_SUFFIX and not name.endswith(CSV_SUFFIX):
            continue
        # 完了マーカー方式を使うならここで除外・判定
        # if READY_MARKER_SUFFIX and name.endswith(READY_MARKER_SUFFIX):
        #     # マーカー自体は処理対象にしない
        #     continue
        yield name


def _try_acquire_lease(bc: BlobClient, seconds: int = 30) -> Optional[BlobLeaseClient]:
    """
    二重実行対策: 短いリースを取り、処理中の重複を防ぐ
    """
    try:
        lease = BlobLeaseClient(bc)
        lease.acquire(lease_duration=seconds)
        return lease
    except Exception as e:
        logging.info(f"Lease acquire skipped for {bc.blob_name}: {e}")
        return None


# def _download_text(bc: BlobClient) -> str:
#     downloader: StorageStreamDownloader = bc.download_blob()
#     return downloader.readall().decode("utf-8")


# def _process(text: str) -> str:
#     """
#     ここがアプリ固有の加工ロジック。
#     例として「行数と文字数を付けて返す」ダミー処理。
#     """
#     lines = text.splitlines()
#     return f"# processed at {dt.datetime.utcnow().isoformat()}Z\nlines={len(lines)} chars={len(text)}\n\n{text}"


# def _upload_text(bc: BlobClient, text: str, *, overwrite=True, metadata: dict | None = None):
#     bc.upload_blob(text.encode("utf-8"), overwrite=overwrite, metadata=metadata)


# def _move_to_archive(svc: BlobServiceClient, src_cc: ContainerClient, blob_name: str):
#     """
#     アーカイブへ移動（コピー＋元削除）
#     """
#     src = src_cc.get_blob_client(blob_name)
#     dst = svc.get_blob_client(CONTAINER_ARCH, blob_name)
#     dst.start_copy_from_url(src.url)
#     # コピー完了までは非同期だが、Azrite/小容量なら短時間。厳密に待ちたい場合は copy status をポーリング。
#     src.delete_blob(delete_snapshots="include")

def count_files_in_container(cc: ContainerClient, suffix: str) -> int:
    """
    指定したコンテナ内にあるファイルの数をカウントする。

    Parameters
    ----------
    connection_string : str
        Azure Storage または Azurite の接続文字列
    container_name : str
        対象のコンテナ名（例: "container"）

    Returns
    -------
    int
        コンテナ内の .txt ファイル数
    """
    # .txt ファイルをカウント
    count = 0
    for blob in cc.list_blobs():
        if blob.name.lower().endswith(suffix.lower()):
            count += 1

    return count

def run_pipeline() -> None:
    svc = BlobServiceClient.from_connection_string(CONN_STR)
    container = svc.get_container_client(CONTAINER_NAME)
    logging.info(f"Processing container: {CONTAINER_NAME}")

    # コンテナが無ければ作っておく（初回起動の利便性）
    for c in (container, svc.get_container_client(CONTAINER_NAME)):
        try:
            c.create_container()
        except Exception:
            pass

    for blob_name in _list_target_blobs(container):
        in_blob = container.get_blob_client(blob_name)

        try:
            text_count = count_files_in_container(container, TXT_SUFFIX)
            logging.info(f"Text file count in container: {text_count}")
            csv_count = count_files_in_container(container, CSV_SUFFIX)
            logging.info(f"CSV file count in container: {csv_count}")
        except Exception as e:
            logging.exception(f"Failed processing {blob_name}: {e}")
        

        for blob_name in _list_target_blobs(container):
            logging.info(f"Found blob: {blob_name}")
            in_blob = container.get_blob_client(blob_name)

            # Django のアップロード完了待ち：ファイルサイズがしばらく安定している等を厳密に見るなら
            # メタデータの READY フラグや「.done」ファイルを使うのが堅実。ここでは簡易にリースで競合回避。
            lease = _try_acquire_lease(in_blob)
            if not lease:
                continue  # ほかのワーカー/インスタンスが処理中

            try:
                logging.info(f"Processing blob: {blob_name}")
                # 必要な処理をここに書く（ダウンロード、加工、アップロード等）
            except Exception as e:
                logging.exception(f"Failed processing {blob_name}: {e}")
            finally:
                try:
                    lease.release()
                except Exception:
                    pass

def main(priceTrigger: func.TimerRequest) -> None:
    utc_now = dt.datetime.utcnow()
    if priceTrigger.past_due:
        logging.warning("Timer is running late!")
    logging.info(f"Timer trigger executed at {utc_now.isoformat()}Z")

    try:
        _ = run_pipeline()
    except Exception as e:
        logging.exception(f"Pipeline failed: {e}")
        raise
