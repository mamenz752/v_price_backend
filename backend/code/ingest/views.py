from django.conf import settings
from django.http import HttpResponse, Http404
from django.shortcuts import render
from django.views import View
from django.views.generic import TemplateView
from azure.storage.blob import BlobServiceClient


def _container_client():
    bsc = BlobServiceClient.from_connection_string(settings.AZURE_CONNECTION_STRING)
    return bsc.get_container_client(settings.AZURE_CONTAINER)

class IndexView(TemplateView):
    template_name = "ingest/index.html"

class TxtListView(TemplateView):
    template_name = "ingest/txt_list.html"

    def get(self, request):
        container = _container_client()
        # txtのみを対象（必要なければ name_starts_with を使わず全部）
        prefix = "txt/"  # 例: "txt/"
        # サーバー側で prefix 絞り込み
        blobs_iter = container.list_blobs(name_starts_with=prefix)
        # .txt のみ、リンク用に prefix を除いた相対名も持たせる
        items = []
        for b in blobs_iter:
            name = str(b.name)
            if not name.lower().endswith(".txt"):
                continue
            rel = name[len(prefix):] if name.startswith(prefix) else name
            items.append({"full": name, "rel": rel})
        # 表示の見やすさでソート（任意）
        items.sort(key=lambda x: x["rel"])
        return render(request, self.template_name, {"items": items})

class TxtDetailView(View):
   def get(self, request, name: str):
        if ".." in name:
            raise Http404("Invalid path")
        prefix = "txt/"
        full_name = f"{prefix}{name}"
        print(full_name)
        container = _container_client()
        try:
            blob = container.get_blob_client(full_name)
            data = blob.download_blob().readall().decode("utf-8", errors="replace")
        except Exception:
            raise Http404("Not found")
        return HttpResponse(data, content_type="text/plain; charset=utf-8")