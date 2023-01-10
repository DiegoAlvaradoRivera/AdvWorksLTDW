
set nocount on;
go 

/*
TRANSACTION NUMBER 1

Ship the order of customer 29781.
Update the ship date, due date, and status.
*/
begin transaction;

declare @order_id int;

select @order_id = SalesOrderID 
from SalesLT.SalesOrderHeader 
where CustomerID = 29781 and Status = 1;

update SalesLT.SalesOrderHeader
set 
ShipDate = integration.EasternTime(), 
DueDate = DATEADD(DAY, 7, integration.EasternTime()), 
Status = 5
where SalesOrderID = @order_id;

commit; 
go 

/*
TRANSACTION NUMBER 2

Cancel the order of the customer 29847.
*/
begin transaction;

declare @order_id int;

select @order_id = SalesOrderID 
from SalesLT.SalesOrderHeader 
where CustomerID = 29847 and Status = 1;

update SalesLT.SalesOrderHeader
set 
Status = 6
where SalesOrderID = @order_id;

delete from SalesLT.SalesOrderDetail 
where SalesOrderID = @order_id;

commit;
go 

print 'SUCCESSFULLY COMPLETED OPERATIONS FOR DAY 2';
go 
