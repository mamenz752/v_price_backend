# accounts/management/commands/seed_users.py
from django.core.management.base import BaseCommand
from django.contrib.auth.models import User, Group

class Command(BaseCommand):
    help = "Insert dummy admin and role-based users"

    def handle(self, *args, **options):
        # グループを作成
        admins_group, _ = Group.objects.get_or_create(name="Admins")
        monitors_group, _ = Group.objects.get_or_create(name="Monitors")

        # 管理者ユーザー（superuser）
        if not User.objects.filter(username="admin_user").exists():
            admin_user = User.objects.create_superuser(
                username="admin_user",
                email="admin@example.com",
                password="adminpass",
            )
            admin_user.is_staff = True
            admin_user.save()
            self.stdout.write(self.style.SUCCESS("Superuser 'admin_user' created"))
        else:
            self.stdout.write("Superuser 'admin_user' already exists")

        # Admins グループに所属するユーザー
        if not User.objects.filter(username="group_admin").exists():
            group_admin = User.objects.create_user(
                username="group_admin",
                email="group_admin@example.com",
                password="testpass",
                is_staff=True,  # 管理画面に入れる
            )
            group_admin.groups.add(admins_group)
            self.stdout.write(self.style.SUCCESS("User 'group_admin' (Admins group) created"))
        else:
            self.stdout.write("User 'group_admin' already exists")

        # Monitors グループに所属するユーザー
        if not User.objects.filter(username="monitor_user").exists():
            monitor_user = User.objects.create_user(
                username="monitor_user",
                email="monitor@example.com",
                password="testpass",
                is_staff=False,  # 管理画面には入れない
            )
            monitor_user.groups.add(monitors_group)
            self.stdout.write(self.style.SUCCESS("User 'monitor_user' (Monitors group) created"))
        else:
            self.stdout.write("User 'monitor_user' already exists")
