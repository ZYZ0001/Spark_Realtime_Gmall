package com.atguigu.gmall.realtime.util

import java.util
import java.util.{Objects, Properties}

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core._

object MyESUtil {
    private val pro: Properties = PropertiesUtil.load("config.properties")
    private var factory: JestClientFactory = null

    /**
      * 获取客户端
      *
      * @return
      */
    def getClient: JestClient = {
        if (factory == null) build()
        factory.getObject
    }

    /**
      * 创建客户端
      *
      * @return
      */
    private def build() = {
        factory = new JestClientFactory
        factory.setHttpClientConfig(new HttpClientConfig.Builder(pro.getProperty("es.host") + ":" + pro.getProperty("es.port"))
            .multiThreaded(true)
            .maxTotalConnection(20)
            .readTimeout(10000)
            .connTimeout(10000)
            .build())
    }

    /**
      * 关闭客户端
      *
      * @param client
      */
    def close(client: JestClient) = {
        if (!Objects.isNull(factory)) {
            try {
                client.shutdownClient()
            } catch {
                case e: Exception => e.printStackTrace()
            }
        }
    }

    /**
      * 批量插入数据到ES
      *
      * @param indexName
      * @param docList
      */
    def insertBulk(indexName: String,  docList: List[(String, Any)]) = {
        if (docList.size > 0) {
            val client: JestClient = getClient
            val builder = new Bulk.Builder().defaultIndex(indexName).defaultType("_doc")

            for ((id, source) <- docList) {
                // 创建index的Action
                val index: Index = new Index.Builder(source).id(id).build()
                // 添加Action
                builder.addAction(index)
            }

            // 执行Action
            val result: BulkResult = client.execute(builder.build)

            // 输出保存的数据条数
            val items: util.List[BulkResult#BulkResultItem] = result.getItems
            println(s"保存的数据条数: ${items.size()}")

            close(client)
        }
    }
}
