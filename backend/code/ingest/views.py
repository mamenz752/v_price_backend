from django.conf import settings
from django.http import HttpResponse, Http404, JsonResponse
from django.shortcuts import render, redirect
from django.views import View
from django.views.generic import TemplateView
from django.contrib import messages
from azure.storage.blob import BlobServiceClient
import csv
from io import StringIO
import traceback
import os
import logging


logger = logging.getLogger(__name__)

def _container_client():
    conn_str = settings.AZURE_CONNECTION_STRING
    container_name = settings.AZURE_CONTAINER
    
    print(f"接続文字列: {conn_str[:20]}... (一部のみ表示)")
    print(f"使用コンテナ: {container_name}")
    
    bsc = BlobServiceClient.from_connection_string(conn_str)
    container = bsc.get_container_client(container_name)
    return container

    
class BlobListView(View):
    """
    Azurite内の全ファイル一覧を表示するビュー
    デバッグ用に作成
    """
    def get(self, request):
        try:
            container = _container_client()
            blobs = []
            
            # 全BLOBを取得
            for blob in container.list_blobs():
                size = blob.size if hasattr(blob, 'size') else 'unknown'
                blobs.append({
                    'name': blob.name,
                    'size': size,
                    'last_modified': blob.last_modified.isoformat() if hasattr(blob, 'last_modified') else 'unknown'
                })
                
            # プレフィックス別にグループ化
            grouped = {
                'price': [b for b in blobs if b['name'].startswith(settings.INGEST_PREFIX_PRICE)],
                'weather': [b for b in blobs if b['name'].startswith(settings.INGEST_PREFIX_WEATHER)],
                'other': [b for b in blobs if not (b['name'].startswith(settings.INGEST_PREFIX_PRICE) or 
                                                b['name'].startswith(settings.INGEST_PREFIX_WEATHER))]
            }
            
            # JSON形式でファイル一覧を返す
            return JsonResponse({
                'blobs': blobs,
                'grouped': grouped,
                'price_prefix': settings.INGEST_PREFIX_PRICE,
                'weather_prefix': settings.INGEST_PREFIX_WEATHER,
            })
        except Exception as e:
            tb = traceback.format_exc()
            return HttpResponse(f"エラー: {str(e)}\n\n{tb}", content_type="text/plain; charset=utf-8")
