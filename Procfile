release: cd backend/code && python manage.py migrate && python manage.py create_admin_from_env
web: cd backend/code && gunicorn config.wsgi:application \
  --workers 2 \
  --threads 4 \
  --worker-class gthread \
  --timeout 25 \
  --log-file -