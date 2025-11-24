"""
Azure Blob内のファイル一覧を確認するスクリプト
"""
from config.storage.azure_blob import get_blob_service_client
from django.conf import settings

def list_blobs():
    try:
        container_name = settings.AZURE_CONTAINER
        container_client = get_blob_service_client()
        # コンテナが存在するか確認
        try:
            container_props = container_client.get_container_properties()
            print(f"コンテナが存在します: {container_name}")
        except Exception as e:
            print(f"コンテナが見つかりません: {str(e)}")
            return
        
        # プレフィックスなしですべてのBlobをリスト
        print("すべてのBlobをリスト:")
        all_blobs = list(container_client.list_blobs())
        print(f"Blob総数: {len(all_blobs)}")
        for i, blob in enumerate(all_blobs[:10]):  # 最初の10個だけ表示
            print(f"  {i+1}. {blob.name}")
        
        # price/プレフィックスでBlobをリスト
        price_prefix = settings.INGEST_PREFIX_PRICE
        print(f"\nprice/プレフィックスでBlobをリスト (プレフィックス: {price_prefix}):")
        price_blobs = list(container_client.list_blobs(name_starts_with=price_prefix))
        print(f"price/ Blob数: {len(price_blobs)}")
        for i, blob in enumerate(price_blobs[:10]):  # 最初の10個だけ表示
            print(f"  {i+1}. {blob.name}")
        
        # txt/プレフィックスでBlobをリスト
        print("\ntxt/プレフィックスでBlobをリスト:")
        txt_blobs = list(container_client.list_blobs(name_starts_with="txt/"))
        print(f"txt/ Blob数: {len(txt_blobs)}")
        for i, blob in enumerate(txt_blobs[:10]):  # 最初の10個だけ表示
            print(f"  {i+1}. {blob.name}")
            
        # 他のプレフィックスも試してみる
        other_prefixes = ["", "price", "txt", "data/", "data/price/", "2019/"]
        for prefix in other_prefixes:
            print(f"\n'{prefix}'プレフィックスでBlobをリスト:")
            prefix_blobs = list(container_client.list_blobs(name_starts_with=prefix))
            print(f"'{prefix}' Blob数: {len(prefix_blobs)}")
            for i, blob in enumerate(prefix_blobs[:5]):  # 最初の5個だけ表示
                print(f"  {i+1}. {blob.name}")
        
    except Exception as e:
        import traceback
        print(f"エラーが発生しました: {str(e)}")
        print(traceback.format_exc())

if __name__ == "__main__":
    list_blobs()