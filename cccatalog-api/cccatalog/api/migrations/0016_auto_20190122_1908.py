# Generated by Django 2.0.8 on 2019-01-22 19:08

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0015_contentsource_notes'),
    ]

    operations = [
        migrations.AlterField(
            model_name='contentsource',
            name='created_on',
            field=models.DateTimeField(),
        ),
    ]
