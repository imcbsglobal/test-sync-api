from django.db import models


class AccUsers(models.Model):
    id = models.CharField(max_length=30, primary_key=True)
    pass_field = models.CharField(max_length=100, db_column='pass')
    role = models.CharField(max_length=30, null=True, blank=True)

    class Meta:
        db_table = 'acc_users'
        managed = False


class AccInvMast(models.Model):
    slno = models.DecimalField(
        max_digits=10, decimal_places=0, primary_key=True)
    invdate = models.DateField(null=True, blank=True)

    class Meta:
        db_table = 'acc_invmast'
        managed = False


class AccInvDetails(models.Model):
    invno = models.DecimalField(max_digits=10, decimal_places=0)
    code = models.CharField(max_length=30, primary_key=True)
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
    basicprice = models.DecimalField(max_digits=14, decimal_places=5, null=True, blank=True)
    partqty = models.DecimalField(max_digits=15, decimal_places=5, null=True, blank=True)

    class Meta:
        db_table = 'acc_product'
        managed = False


class AccPurchaseMaster(models.Model):
    slno = models.DecimalField(max_digits=10, decimal_places=0, primary_key=True)
    date = models.DateField(null=True, blank=True)
    pdate = models.DateField(null=True, blank=True)

    class Meta:
        db_table = 'acc_purchasemaster'
        managed = False


class AccPurchaseDetails(models.Model):
    billno = models.DecimalField(max_digits=10, decimal_places=0, primary_key=True)
    code = models.CharField(max_length=30)
    quantity = models.DecimalField(max_digits=15, decimal_places=5)

    class Meta:
        db_table = 'acc_purchasedetails'
        managed = False


class AccProduction(models.Model):
    productionno = models.DecimalField(max_digits=20, decimal_places=0, primary_key=True)
    date = models.DateField(null=True, blank=True)

    class Meta:
        db_table = 'acc_production'
        managed = False


class AccProductionDetails(models.Model):
    masterno = models.DecimalField(max_digits=30, decimal_places=0, primary_key=True)
    code = models.CharField(max_length=30)
    qty = models.DecimalField(max_digits=15, decimal_places=5)

    class Meta:
        db_table = 'acc_productiondetails'
        managed = False