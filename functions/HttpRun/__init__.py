import json
import logging
import azure.functions as func

# Timer と同じ処理を再利用
from ..TimerWriteBlob.__init__ import run_pipeline

logging.getLogger().setLevel(logging.INFO)

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("[HTTP] invoked")
    try:
        result = run_pipeline()
        return func.HttpResponse(
            json.dumps({"ok": True, "result": result}, ensure_ascii=False),
            status_code=200,
            mimetype="application/json",
        )
    except Exception as e:
        logging.exception("[HTTP] failed: %s", e)
        return func.HttpResponse(
            json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False),
            status_code=500,
            mimetype="application/json",
        )
