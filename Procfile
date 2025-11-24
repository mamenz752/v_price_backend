release: cd backend && python manage.py migrate
web: cd backend && gunicorn config.wsgi:application --log-file - --timeout 360
