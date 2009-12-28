(ns humpty-dumpty-spec
  (:use [clojure.test :only [run-tests deftest is]])
  (:use org.rathore.amit.humpty-dumpty.core))

(def redis-server-spec {:host "127.0.0.1" :db 8})

(binding [*redis-server-spec* redis-server-spec]

  (defdumpty consumer
    (string-type :cid :merchant-id :session-start-time :url-referrer)
    (list-type :cart-items))
  
  (defdumpty consumer-json
    (format-type :json)
    (string-type :cid :merchant-id :session-start-time :url-referrer)
    (list-type :cart-items))
  

  (def adi (consumer :new))
  
  (adi :set! :cid "abcdef")
  (adi :set! :merchant-id "14")
  (def start-time (System/currentTimeMillis))
  (adi :set! :session-start-time start-time)
  (adi :set! :url-referrer "google.com")
  (def item-1 {:cost 10.95 :sku "XYZ"})
  (def item-2 {:cost 22.40 :sku "RST"})
  (adi :add! :cart-items item-1)
  (adi :add! :cart-items item-2)

  (deftest test-format-spec
    (is (= (consumer-json :format) :json)))

  (deftest test-dumpty-definition
    (is (ifn? consumer))
    (is (= (consumer :format) :clj-str))
    (is (= (consumer :name) 'consumer)))
  
  (deftest test-consumer-object
    (is (= (adi :type) consumer))
    (is (= (adi :get :cid) "abcdef"))
    (is (= (adi :get :merchant-id) "14"))
    (is (= (adi :get :session-start-time) start-time))
    (is (= (adi :get :url-referrer) "google.com"))
    (is (= (count (adi :get :cart-items)) 2))
    (is (= (adi :get :cart-items) (apply list [item-2 item-1])))
    )
  
)


