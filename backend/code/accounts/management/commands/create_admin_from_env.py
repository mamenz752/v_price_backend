from django.core.management.base import BaseCommand
from django.contrib.auth import get_user_model
import os


class Command(BaseCommand):
    help = "Create a superuser from environment variables: ADMIN_USERNAME, ADMIN_EMAIL, ADMIN_PASSWORD"

    def add_arguments(self, parser):
        parser.add_argument(
            "--force",
            action="store_true",
            help="If set, overwrite existing user with same username/email by resetting password and ensuring superuser/staff flags.",
        )

    def handle(self, *args, **options):
        username = os.environ.get("ADMIN_USERNAME")
        email = os.environ.get("ADMIN_EMAIL")
        password = os.environ.get("ADMIN_PASSWORD")

        if not username or not email or not password:
            self.stdout.write(self.style.ERROR(
                "Environment variables ADMIN_USERNAME, ADMIN_EMAIL and ADMIN_PASSWORD must all be set."
            ))
            return

        User = get_user_model()

        try:
            user = User.objects.filter(username=username).first()
            if user:
                if options.get("force"):
                    user.email = email
                    user.is_staff = True
                    user.is_superuser = True
                    user.set_password(password)
                    user.save()
                    self.stdout.write(self.style.SUCCESS(f"Updated existing user '{username}' as superuser."))
                else:
                    self.stdout.write(self.style.WARNING(
                        f"User '{username}' already exists. Use --force to update password/flags."
                    ))
                return

            # If no user with that username, check by email
            user_by_email = User.objects.filter(email=email).first()
            if user_by_email:
                if options.get("force"):
                    user_by_email.username = username
                    user_by_email.is_staff = True
                    user_by_email.is_superuser = True
                    user_by_email.set_password(password)
                    user_by_email.save()
                    self.stdout.write(self.style.SUCCESS(f"Updated existing user with email '{email}' as superuser."))
                else:
                    self.stdout.write(self.style.WARNING(
                        f"A user with email '{email}' already exists. Use --force to update password/flags."
                    ))
                return

            # Create new superuser
            user = User.objects.create_user(username=username, email=email, password=password)
            user.is_staff = True
            user.is_superuser = True
            user.save()

            self.stdout.write(self.style.SUCCESS(f"Created superuser '{username}' from environment variables."))

        except Exception as e:
            self.stderr.write(str(e))
            raise
