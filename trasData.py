from clsConexion import clsConexion
import sys
import pymongo

conex = clsConexion()

conexi = pymongo.MongoClient(host=['localhost:27017'])
db = conexi.NorthWind


def listCateg():
    regis = conex.consultar("SELECT c.CategoryID, c.CategoryName, p.ProductID, p.ProductName \
         FROM categories c \
         INNER JOIN products p ON c.CategoryID = p.CategoryID \
         ORDER BY c.CategoryID")

    for tupla in regis:
        print('Id Categoria........:{0}'.format(tupla['CategoryID']))
        print('Nombre de categoria........:{0}'.format(tupla['CategoryName']))
        print('Id producto....:{0}'.format(tupla['ProductID']))
        print('Nombre producto....:{0}'.format(tupla['ProductName']))

        print('---------------------------------------')


def inventProd():
    regis = conex.consultar(" SELECT p.ProductID, p.ProductName, p.UnitPrice, p.UnitsInStock, (p.UnitPrice * p.UnitsInStock) AS Total FROM products p INNER JOIN categories c ON p.CategoryID = c.CategoryID WHERE c.CategoryID = p.CategoryID")


    for tupla in regis:
        print('Id producto........:{0}'.format(tupla['ProductID']))
        print('Nombre de producto........:{0}'.format(tupla['ProductName']))
        print('Unit price....:{0}'.format(tupla['UnitPrice']))
        print('Unit Stock....:{0}'.format(tupla['UnitsInStock']))
        print('Total....:{0}'.format(tupla['Total']))

        print('---------------------------------------')

# create CRUD process to mongoDB
def insCat():
    regis = conex.consultar('SELECT * FROM orders  JOIN order_details ON orders.OrderID = order_details.OrderID JOIN customers ON orders.CustomerID = customers.CustomerID JOIN employees ON orders.EmployeeID = employees.EmployeeID  JOIN products ON order_details.ProductID = products.ProductID' )
    for tupla in regis:
        try:
            db.order.insert_one({
                "OrderId": tupla['OrderID'],
                "CustomerID": tupla['CustomerID'],
                "EmployeeID": tupla['EmployeeID'],
                "OrderDate": tupla['OrderDate'],
                "RequiredDate": tupla['RequiredDate'],
                "ShippedDate": tupla['ShippedDate'],
                "ShipVia": tupla['ShipVia'],
                "ShipName": tupla['ShipName'],
                "ShipCity": tupla['ShipCity'],
                "ShipRegion": tupla['ShipRegion'],


                # OrderDetails
                 "ProductID": tupla['ProductID'],
                 "Quantity": tupla['Quantity'],
                 "Discount": tupla['Discount'],
                #Products
                "ProductName": tupla['ProductName'],
                "SupplierID": tupla['SupplierID'],
                "CategoryID": tupla['CategoryID'],
                "QuantityPerUnit": tupla['QuantityPerUnit'],
                "UnitsInStock": tupla['UnitsInStock'],
                "UnitsOnOrder": tupla['UnitsOnOrder'],
                "ReorderLevel": tupla['ReorderLevel'],
                "Discontinued": tupla['Discontinued'],
                #Customers
                "CompanyName": tupla['CompanyName'],
                "ContactName": tupla['ContactName'],
                "ContactTitle": tupla['ContactTitle'],
                "Address": tupla['Address'],
                "City": tupla['City'],
                "Region": tupla['Region'],
                "PostalCode": tupla['PostalCode'],
                "Country": tupla['Country'],
                "Phone": tupla['Phone'],
                "Fax": tupla['Fax'],
                # employees
                "LastName": tupla['LastName'],
                "FirstName": tupla['FirstName'],
                "Title": tupla['Title'],
                "TitleOfCourtesy": tupla['TitleOfCourtesy'],
                "BirthDate": tupla['BirthDate'],
                "HireDate": tupla['HireDate'],
                "Address": tupla['Address'],
                "City": tupla['City'],
                "Region": tupla['Region'],
                "PostalCode": tupla['PostalCode'],
                "Country": tupla['Country'],
                "HomePhone": tupla['HomePhone'],
                "Extension": tupla['Extension'],
                "Photo": tupla['Photo'],
                "Notes": tupla['Notes'],
                "ReportsTo": tupla['ReportsTo']




            })
        except Exception as e:
            print("Error: {0} {1}".format(type(e), e))





inventProd()
