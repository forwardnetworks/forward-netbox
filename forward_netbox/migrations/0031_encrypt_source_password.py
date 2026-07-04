from django.db import migrations


def encrypt_passwords(apps, schema_editor):
    from forward_netbox.utilities.crypto import encrypt_secret
    from forward_netbox.utilities.crypto import is_encrypted

    ForwardSource = apps.get_model("forward_netbox", "ForwardSource")
    for source in ForwardSource.objects.all():
        parameters = source.parameters or {}
        password = parameters.get("password")
        if password and not is_encrypted(password):
            parameters = dict(parameters)
            parameters["password"] = encrypt_secret(password)
            source.parameters = parameters
            source.save(update_fields=["parameters"])


def decrypt_passwords(apps, schema_editor):
    from forward_netbox.utilities.crypto import decrypt_secret
    from forward_netbox.utilities.crypto import is_encrypted

    ForwardSource = apps.get_model("forward_netbox", "ForwardSource")
    for source in ForwardSource.objects.all():
        parameters = source.parameters or {}
        password = parameters.get("password")
        if is_encrypted(password):
            parameters = dict(parameters)
            parameters["password"] = decrypt_secret(password)
            source.parameters = parameters
            source.save(update_fields=["parameters"])


class Migration(migrations.Migration):
    dependencies = [
        ("forward_netbox", "0030_forwarddeviceanalysis_cve_ids"),
    ]

    operations = [
        migrations.RunPython(encrypt_passwords, decrypt_passwords),
    ]
