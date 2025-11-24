release: cd backend/code && python manage.py migrate
web: cd backend/code && gunicorn config.wsgi:application --log-file - --timeout 360
