release: cd backend/code && python manage.py migrate && python manage.py create_admin_for_env
web: cd backend/code && gunicorn config.wsgi:application --log-file - --timeout 360
