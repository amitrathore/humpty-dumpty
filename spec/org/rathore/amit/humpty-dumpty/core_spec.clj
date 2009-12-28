(ns humpty-dumpty-spec
  (:use [clojure.test :only [run-tests deftest is]])
  (:use org.rathore.amit.humpty-dumpty.core)
  (:use org.rathore.amit.humpty-dumpty.persistence)
)

(def redis-server-spec {:host "127.0.0.1" :db 8})

(binding [*redis-server-spec* redis-server-spec]

  (defdumpty consumer
    (string-type :cid :merchant-id :session-start-time :url-referrer)
    (list-type :cart-items)
    (primary-key :cid :merchant-id))
  
  (defdumpty consumer-json
    (format-type :json)
    (key-separator "***")
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

  (deftest test-override-spec
    (is (= (consumer-json :format) :json))
    (is (= (consumer-json :key-separator) "***")))

  (deftest test-dumpty-definition
    (is (ifn? consumer))
    (is (= (consumer :format) :clj-str))
    (is (= (consumer :name) 'consumer))
    (is (= (consumer :key-type :cid) :string-type))
    (is (= (consumer :key-type :cart-items) :list-type))
    (is (= (consumer :primary-key) '(:cid :merchant-id)))
    (is (= (consumer :string-keys ["abcdef" "14"]) '("abcdef___14___:cid" "abcdef___14___:merchant-id" "abcdef___14___:session-start-time" "abcdef___14___:url-referrer")))
    (is (= (consumer :list-keys ["abcdef" "14"]) '("abcdef___14___:cart-items"))))
  
  (deftest test-consumer-object
    (is (= (adi :type) consumer))
    (is (= (adi :primary-key-value) "abcdef___14"))
    (is (= (adi :get :cid) "abcdef"))
    (is (= (adi :get :merchant-id) "14"))
    (is (= (adi :get :session-start-time) start-time))
    (is (= (adi :get :url-referrer) "google.com"))
    (is (= (count (adi :get :cart-items)) 2))
    (is (= (adi :get :cart-items) (apply list [item-2 item-1])))
    )

  (deftest test-persistable-for
    (let [persistable (persistable-for adi)]
      (is (= (:key-type (persistable "abcdef___14___:cid")) :string-type))
      (is (= (:value (persistable "abcdef___14___:cid")) "\"abcdef\""))
      (is (= (:value (persistable "abcdef___14___:merchant-id")) "\"14\""))
      (is (= (:value (persistable "abcdef___14___:session-start-time")) (str start-time)))
      (is (= (:value (persistable "abcdef___14___:url-referrer")) "\"google.com\""))

      (is (= (:key-type (persistable "abcdef___14___:cart-items")) :list-type))
      (is (= (:value (persistable "abcdef___14___:cart-items")) '("{:cost 22.4, :sku \"RST\"}" "{:cost 10.95, :sku \"XYZ\"}")))))


  (defn fetch-for-test [key-type key]
    (((fetchers key-type) key) key))

  (deftest test-real-saving
    (redis/flushdb)
    (adi :save!)
    (is (= (fetch-for-test :string-type "abcdef___14___:cid") "\"abcdef\""))
    (is (= (fetch-for-test :string-type "abcdef___14___:merchant-id") "\"14\""))
    (is (= (fetch-for-test :string-type "abcdef___14___:url-referrer") "\"google.com\""))
    (is (= (fetch-for-test :string-type "abcdef___14___:session-start-time") (str start-time)))
    (is (= (fetch-for-test :list-type "abcdef___14___:cart-items") ["{:cost 22.4, :sku \"RST\"}" "{:cost 10.95, :sku \"XYZ\"}"])))

  (deftest test-real-thawing
    (redis/flushdb)
    (adi :save!)
    (let [new-adi (consumer :find "abcdef" "14")]
      (println new-adi)))
)


(defn run-humpty-dumpty-tests []
  (redis/with-server redis-server-spec (run-tests))) 