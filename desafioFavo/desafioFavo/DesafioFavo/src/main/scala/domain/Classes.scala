package domain

case class Orders(buyer_id : String,
                  date : String,
                  order_id : String,
                  seller_id : String,
                  product : List[product]
                )

case class OrdersWithDetails(buyer_id : String,
                             date : String,
                             order_id : String,
                             seller_id : String,
                             product : List[productOrder],
                             value : Double
                 )

case class product(sku : BigInt, qt : BigInt )
case class productOrder(sku : BigInt, qt : BigInt , name : String, price :Double, value : Double)
case class productDetails(sku : BigInt, id : String , name : String, price :Double)