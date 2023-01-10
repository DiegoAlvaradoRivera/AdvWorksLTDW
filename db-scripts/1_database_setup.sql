
set nocount on; 
go 

/*
Correct sales order header subtotals. According to the column description, 
SalesOrderHeader.SubTotal is computed as SUM(SalesOrderDetail.LineTotal) 
for the appropriate SalesOrderID, however this does not hold.
*/

begin transaction;

with 
calculated_subtotals as (
select SalesOrderID, sum(LineTotal) as CalculatedSubTotal from SalesLT.SalesOrderDetail group by SalesOrderID
)
update SalesLT.SalesOrderHeader 
set SubTotal = CS.CalculatedSubTotal
from 
SalesLT.SalesOrderHeader as SOH
left join calculated_subtotals as CS
on SOH.SalesOrderID = CS.SalesOrderID

commit; 
go

/*
Delete customers that have no placed orders
*/
begin transaction;

declare @customers_without_sales table (
CustomerID INT
);

insert into @customers_without_sales(CustomerId) 
select CustomerID from SalesLT.Customer
except
select CustomerID from SalesLT.SalesOrderHeader;

delete from SalesLT.CustomerAddress
where CustomerID in (select CustomerID from @customers_without_sales);

delete from SalesLT.Customer
where CustomerID in (select CustomerID from @customers_without_sales);

commit;
go

print 'SUCCESSFULLY MODIFIED DATABASE';
go 
