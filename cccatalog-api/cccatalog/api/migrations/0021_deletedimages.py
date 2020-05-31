# Generated by Django 2.2.4 on 2020-01-16 18:56

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0020_auto_20190918_1954'),
    ]

    operations = [
        migrations.CreateModel(
            name='DeletedImages',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('created_on', models.DateTimeField(auto_now_add=True)),
                ('updated_on', models.DateTimeField(auto_now=True)),
                ('deleted_id', models.UUIDField(db_index=True, help_text='The identifier of the deleted image.', unique=True)),
                ('deleting_user', models.CharField(help_text='The user that deleted the image.', max_length=50)),
            ],
            options={
                'abstract': False,
            },
        ),
    ]
