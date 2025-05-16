package app

import java.sql.Timestamp
import java.util.{Calendar, Date, UUID}

import domain.{Orders, OrdersWithDetails, productDetails, productOrder}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.{Window, WindowSpec}


object DesafioFavo {

  /// PARÂMETROS PARA OTIMIZAÇÃO DE PERFORMANCE
  // UTILIZAR PREFERENCIALMENTE A ESTRATEGIA Sort Merge Join PARA REDUZIR A QUANTIDADE DE SHUFFLE ENTRE OS WORKERS
  val spark = SparkSession.builder().appName("DesafioFavo")
    .config("spark.sql.join.preferSortMergeJoin", true)
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .config("spark.sql.defaultSizeInBytes", 100000)
    .config("spark.sql.shuffle.partitions", 20)
    .getOrCreate()



  val sc = SparkContext.getOrCreate()
  sc.setLogLevel("ERROR")
  import spark.implicits._

  def main(args : Array[String]) = {

    val path = args(0)
    val readPath = path + "Input"
    val  dataExecucao = new Timestamp(new Date().getTime).toString.replaceAll(" ", "").replaceAll(":", "-")
    val writePath: String = path + "Output/" + dataExecucao


    println("Diretório de leitura " + readPath)
    println("Diretório de saída " + writePath)

    println("INICIO DO PROCESSAMENTO   " + new Timestamp(new Date().getTime))


    //declaro os schema dos jasons, declarando o spark não precisa identificar o tipo de dado, melhora de performance para grandes arquivos
    val buyersSchema = "buyer STRING ,phone STRING ,buyer_id STRING ,start STRING"
    val productsSchema = "name STRING ,sku INTEGER ,id STRING ,price DOUBLE"
    val sellersSchema = "seller STRING ,phone STRING ,seller_id STRING ,start STRING"


    // LEITURA DO ARQUIVO DE CLIENTES
    val buyers: DataFrame = spark.read.schema(buyersSchema).json(readPath + "/buyers.json")
      .withColumn("start", from_unixtime(col("start"), "yyyy-MM-dd")) //FORMATO A DATA PARA O PADRÃO ANO-MES-DIA
      .withColumnRenamed("buyer", "buyer_name") //FORMATO O NOME DA COLUNA
      .withColumnRenamed("phone", "buyer_phone")//FORMATO O NOME DA COLUNA
      .withColumnRenamed("start", "buyer_start").cache()


    // LEITURA DO ARQUIVO DE PRODUTOS
    val products: Dataset[productDetails] = spark.read.schema(productsSchema).json(readPath + "/products.json").as[productDetails].cache()

    // TRANFORMO OS PEDISOS EM UMA LISTA PARA FAZER O ENRIQUECIMENTO DOS PEDIDOS
    val productsDetailsList = products.collect().toList

    // LEITURA DO ARQUIVO DE VENDEDORES
    val sellers: DataFrame = spark.read.schema(sellersSchema).json(readPath + "/sellers.json")
      .withColumn("start", from_unixtime(col("start"), "yyyy-MM-dd")) //FORMATO A DATA PARA O PADRÃO ANO-MES-DIA
      .withColumnRenamed("phone", "seller_phone")
      .withColumnRenamed("start", "seller_start").cache()

    // LEITURA DO ARQUIVO DE PEDIDOS
    val orders: Dataset[Orders] = spark.read.json(readPath + "/orders.json")
      .withColumn("date", from_unixtime(col("date"), "yyyy-MM-dd")).as[Orders].cache() //FORMATO A DATA PARA O PADRÃO ANO-MES-DIA


    // ENRIQUECIMENTO DOS PEDIDOS COM  A INFORMAÇÃO DOS NOMES DOS PRODUTOS, VALOR DOS PRODUTOS E VALOR TOTAL DO PEDIDO
    val ordersWithDetails: Dataset[OrdersWithDetails] = orders.map(o => {


      val productsDetails: List[productOrder] = o.product.map(p => { //PARA CADA PRODUTO DO PEDIDO
        // DA LISTA DE PRODUTOS ENCONTRO O PRODUTO DO PEDIDO
        val productDetail = productsDetailsList.filter(f => f.sku == p.sku).head

        //POPULO UMA CASE CLASS COM O NOME DO PRODUTO, PREÇO UNITARIO E O VALOR NO PEDIDO
        productOrder(sku = p.sku,
          qt = p.qt,
          name = productDetail.name,
          price = productDetail.price,
          value = p.qt.toDouble * productDetail.price)
      })

      // POPULO UMA NOVA CASE CLASSE COM AS INFORMAÇOES ENRIQUECIDAS DOS PEDIDOS E O VALOR TOTAL DO PEDIDO
      OrdersWithDetails(
        buyer_id = o.buyer_id,
        date = o.date,
        order_id = o.order_id,
        seller_id = o.seller_id,
        product = productsDetails,
        value = productsDetails.map(_.value).sum
      )
    }).cache()


    ////1) Gerar um arquivo enriquecido que contenha todas as informações unificadas.
    // REALIZO O JOIN DOS PEDIDOS COM COMPRADORES E VENDEDORES
    val dataBase: DataFrame = ordersWithDetails.as("orders")
      .join(buyers.as("buyers"), ordersWithDetails("buyer_id") === buyers("buyer_id"), "inner")
      .join(sellers.as("sellers"), ordersWithDetails("seller_id") === sellers("seller_id"), "inner")
      .select("buyers.buyer_id", "buyers.buyer_name", "buyers.buyer_phone", "buyers.buyer_start",
        "sellers.seller_id", "sellers.seller", "sellers.seller_phone", "sellers.seller_start",
        "orders.order_id", "orders.date", "orders.product", "orders.value").cache()

    println("TASK 1 - Gerar um arquivo enriquecido que contenha todas as informações unificadas.")
    //dataBase.show(1)

    //AGRUPO TODOS OS PEDIDOS EM UMA UNICA PARTIÇÃO E GRAVO EM UM ARQUIVO JASON
    dataBase.coalesce(1).write.json(writePath)
    println("--------------------------------------------------------------------------------------------------------")
    println(" ")



    ///2) Crescimento das vendas por vendedor semanalmente (encontrar a diferença entre a semana atual analisada e a semana passada, para todos as semanas dos anos)

    println("TASK2 Crescimento das vendas por vendedor semanalmente")

    /// DADO DOS PEDIDOS DESCUBRO QUAL A SEMANA NO ANO O PEDIDO É REFERENTE, APÓS SOMO TODOS OS PEDIDOS DA MESMA SEMANA
    val ordersWithWeeks = ordersWithDetails.select("seller_id", "value", "date")
      .withColumn("week", date_format(col("date"), "w")) // QUAL O NUMERO DA SEMANA QUE O PEDIDO É
      .withColumn("year", date_format(col("date"), "YYYY")) //ANO DO PEDIDO
      .groupBy("seller_id", "week", "year") // AGRUPO OS PEDIDOS PELO VENDEDOR, SEMANA E ANO
      .agg(bround(sum(col("value")), 2).as("vendas_na_semana")) //SOMO OS PEDIDOS DA SEMANA E ARREDONDO O VALOR
      .withColumn("year-week", concat(col("year"), lit('-'), col("week"))) //CONCATENO AS COLUNAS ANO E MES


    // CRIO UMA JANELA PARA PARTICIONAR OS DADOS, PARTIÇÃO SERÁ PELO VENDEDOR ORDENADO PELO ANO-SEMANA
    val window: WindowSpec = Window.partitionBy("seller_id").orderBy("year-week")

    // INCLUO UMA NOVA COLUNA COM O VALOR DA VENDA DA SEMANA ANTERIOR
    val ordersWithPastWeek: DataFrame = ordersWithWeeks.withColumn("semana_anterior", lag("vendas_na_semana", 1).over(window))

    // CALCULO O CRESCIMENTO DO VENDEDOR SEMANA A SEMANA
    val growthSeller = ordersWithPastWeek.na.fill(0) /// PARA OS REGISTRO QUE SÃO A PRIMEIRA VENDA NÃO HÁ SEMANA ANTERIOR, POR ISSO SUBSTITIO O NULO POR 0
      .withColumn("crescimento", bround(col("vendas_na_semana") - col("semana_anterior"), 2)) // CALCULO A DIFERENÇA ENTRE A VENDA DESSA SEMANA E A ANTERIOR
      .select("seller_id", "year", "week", "year-week", "semana_anterior", "vendas_na_semana", "crescimento") //SELECIONO AS COLUNAS NECESSÁRIAS

    growthSeller.show(2000,false)

    println("--------------------------------------------------------------------------------------------------------")
    println(" ")

    //    3) Contribuição de cada vendedor sobre o valor total vendido desde sempre (encontrar o percentual que cada um dos vendedores contribuiu para o total de vendas desde sempre)

    /// ENCONTRO O VALOR TOTAL DE TODAS AS VENDAS REALIZADAS
    val total_sales = dataBase.select(sum(col("value")).as("total_sales").cast("Double")).head().getDouble(0)

    /// AGRUPO TODAS AS VENDAS DO VENDEDOR E DEPOIS ENCONTRO A % QUE REPRESENTA ISSO EM TODAS AS VENDAS
    val contribution_all_sales = dataBase.groupBy(col("seller_id")).agg(sum("value").as("total_sales"))
      .withColumn("contribution_all_sales", ((col("total_sales") / total_sales) * 100))
    //.orderBy(col("contribution_all_sales"))

    println("TASK 3 - Contribuição de cada vendedor sobre o valor total vendido desde sempre ")
    contribution_all_sales.show(2000, false)


    println("--------------------------------------------------------------------------------------------------------")
    println(" ")

    //    4) Existem ordens que não possuem a informação de quem realizou a compra,
    //    elabore uma tabela que possua quem poderia ser o possível comprador de cada uma dessas ordens. Explique a lógica utilizada para chegar nessa informação.
    //
    println("TASK 4 - ordens que não possuem a informação de quem realizou a compra")
    orders.where(col("buyer_id").isNull).orderBy(col("seller_id")).show(10000, false)
    println(" ")


    println("FIM DO PROCESSAMENTO   " + new Timestamp(new Date().getTime))

    //Thread.sleep(100000)


  }
}
