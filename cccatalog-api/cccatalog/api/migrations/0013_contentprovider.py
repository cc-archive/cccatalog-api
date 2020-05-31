# Generated by Django 2.0.8 on 2019-01-22 18:51

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0012_auto_20190102_2012'),
    ]

    operations = [
        migrations.CreateModel(
            name='ContentProvider',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('created_on', models.DateTimeField(auto_now_add=True)),
                ('updated_on', models.DateTimeField(auto_now=True)),
                ('provider_identifier', models.CharField(max_length=50)),
                ('provider_name', models.CharField(max_length=250)),
                ('domain_name', models.CharField(max_length=500)),
                ('filter_content', models.BooleanField(default=False)),
            ],
            options={
                'db_table': 'content_provider',
            },
        ),
    ]
