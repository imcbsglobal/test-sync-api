from django.db import models


class AccInvMast(models.Model):
    slno = models.DecimalField(
        max_digits=10, decimal_places=0, primary_key=True)
    invdate = models.DateField(null=True, blank=True)

    class Meta:
        db_table = 'acc_invmast'
        managed = False


class AccInvDetails(models.Model):
    # Add an auto ID field since this table doesn't have a natural primary key
    id = models.AutoField(primary_key=True)
    invno = models.DecimalField(max_digits=10, decimal_places=0)
    code = models.CharField(max_length=30)
    quantity = models.DecimalField(max_digits=15, decimal_places=5)

    class Meta:
        db_table = 'acc_invdetails'
        managed = False


class AccProduct(models.Model):
    code = models.CharField(max_length=30, primary_key=True)
    name = models.CharField(max_length=200, null=True, blank=True)
    quantity = models.DecimalField(
        max_digits=15, decimal_places=5, null=True, blank=True)
    openingquantity = models.DecimalField(
        max_digits=15, decimal_places=5, null=True, blank=True)
    stockcatagory = models.CharField(max_length=20, null=True, blank=True)
    unit = models.CharField(max_length=10, null=True, blank=True)
    product = models.CharField(max_length=30, null=True, blank=True)
    brand = models.CharField(max_length=30, null=True, blank=True)
    billedcost = models.DecimalField(
        max_digits=14, decimal_places=5, null=True, blank=True)

    class Meta:
        db_table = 'acc_product'
        managed = False
