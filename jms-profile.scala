import org.apache.activemq._
import javax.jms._
import javax.management._
import javax.management.remote._
import javax.management.openmbean._
import collection.mutable.ListBuffer


implicit def to_on(value:String)=new ObjectName(value)

val mbean_server = JMXConnectorFactory.connect(new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:1099/jmxrmi"), null).getMBeanServerConnection
mbean_server.getAttribute("java.lang:type=MemoryPool,name=CMS Old Gen", "Usage")

def mem_usage = mbean_server.getAttribute("java.lang:type=MemoryPool,name=CMS Old Gen", "Usage").asInstanceOf[CompositeData].get("used").asInstanceOf[Long]
def gc = {
  for( i <- 0 until 10 ) {
    mbean_server.invoke("java.lang:type=Memory", "gc", Array(), Array())
    Thread.sleep(200)
  }
}

def createText(messageSize:Int) = {
    var buffer = new StringBuffer(messageSize+26)
    while (buffer.length < messageSize) {
        buffer.append("abcdefghijklmnopqrstuvwxyz")
    }
    buffer.toString.substring(0,messageSize)
}

val factory = new ActiveMQConnectionFactory("tcp://localhost:61616")

var results = ListBuffer[(String, Long)]()

def profile[T](name:String)(setup: =>T)(tear_down: T=>Unit):Unit = {
  println("Setup GC")
  gc
  val start = mem_usage
  println("Loading with: "+name)
  val x = setup
  println("Teardown GC")
  gc
  val end = mem_usage
  tear_down(x)
  
  var size = end-start
  println("result %s: %,d Bytes".format(name, size))
  
  results.append((name, size))
}

def run = {
  
  val outer_connection = factory.createConnection()
  outer_connection.start()
  val outer_session = outer_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
  val outer_producer = outer_session.createProducer(null)
  val test_queue = outer_session.createQueue("TEST")
  val test_topic = outer_session.createTopic("TEST")

  { 
    // Make sure these destinations exist..
    outer_session.createConsumer(test_topic).close
    outer_session.createConsumer(test_queue).close
  }

  def drain(dest:Destination) = {
    val consumer = outer_session.createConsumer(dest)
    def poll = consumer.receive(100)!=null
    var count = 0;
    while (poll) {
      count+=1;
    }
    consumer.close
  }

  profile("100 Connections") {
    0.until(100).map { x=>
      val connection = factory.createConnection()
      connection.start()
      connection
    }
  } { x =>
    x.foreach {
      _.close
    }
  }
  
  profile("100 Connection 100 Session") {
    0.until(100).map { x=>
      val connection = factory.createConnection()
      connection.start()
      val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
      (connection,session)
    }
  } { x =>
    x.foreach {
      _._1.close
    }
  }

  profile("100 Connection 100 Session 100 Topic Producer") {
    0.until(100).map { x=>
      val connection = factory.createConnection()
      connection.start()
      val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
      val destination = test_topic
      val producer = session.createProducer(destination)
      producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT)
      (connection,producer)
    }
  } { x =>
    x.foreach {
      _._1.close
    }
  }

  profile("100 Connection 100 Session 100 Topic Consumer") {
    0.until(100).map { x=>
      val connection = factory.createConnection()
      connection.start()
      val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
      val destination = test_topic
      val consumer = session.createConsumer(destination)
      consumer
      (connection,consumer)
    }
  } { x =>
    x.foreach {
      _._1.close
    }
  }


  profile("100 Topic") {
    val now = System.currentTimeMillis
    0.until(100).foreach { x=>
      outer_producer.send(outer_session.createTopic(now+":"+x), outer_session.createTextMessage(""))
    }
  } { x =>
  }
  
  profile("100 Connection 100 Session 100 Queue Producer") {
    0.until(100).map { x=>
      val connection = factory.createConnection()
      connection.start()
      val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
      val destination = test_queue
      val producer = session.createProducer(destination)
      producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT)
      (connection,producer)
    }
  } { x =>
    x.foreach {
      _._1.close
    }
  }

  profile("100 Connection 100 Session 100 Queue Consumer") {
    0.until(100).map { x=>
      val connection = factory.createConnection()
      connection.start()
      val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
      val destination = test_queue
      val consumer = session.createConsumer(destination)
      consumer
      (connection,consumer)
    }
  } { x =>
    x.foreach {
      _._1.close
    }
  }

  profile("100 Empty Messages") {
    0.until(100).foreach { x=>
      outer_producer.send(test_queue, outer_session.createTextMessage(""))
    }
  } {  x =>
  }
  
  drain(test_queue)
  
  profile("100 1K Messages") {
    val text = createText(1024)
    0.until(100).foreach { x=>
      outer_producer.send(test_queue, outer_session.createTextMessage(text))
    }
  } {  x =>
  }
  
  drain(test_queue)
  
  profile("100 Queues & 100 Empty Messages") {
    val now = System.currentTimeMillis
    0.until(100).foreach { x=>
      outer_producer.send(outer_session.createQueue(now+":"+x), outer_session.createTextMessage(""))
    }
  } { x =>
  }  

  println(results.map(x=> "%s,%d".format(x._1, x._2) ).mkString("\n"))
}

