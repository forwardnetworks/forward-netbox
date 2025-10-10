from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("forward_netbox", "0001_initial"),
    ]

    operations = [
        migrations.DeleteModel(name="ForwardRelationshipField"),
        migrations.DeleteModel(name="ForwardTransformField"),
        migrations.DeleteModel(name="ForwardTransformMap"),
        migrations.DeleteModel(name="ForwardTransformMapGroup"),
    ]
