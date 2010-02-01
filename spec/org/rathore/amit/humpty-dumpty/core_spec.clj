(ns humpty-dumpty-spec
  (:use [clojure.test :only [run-tests deftest is]])
  (:use org.rathore.amit.humpty-dumpty.core)
  (:use org.rathore.amit.humpty-dumpty.persistence)
  (:use org.rathore.amit.humpty-dumpty.utils)
  (:use org.rathore.amit.conjure.core))

(def redis-server-spec {:host "127.0.0.1" :db 8})

(binding [*redis-server-spec* redis-server-spec]

  (defdumpty consumer
    (string-type :cid :merchant-id :session-start-time :url-referrer :client-time :timezone)
    (list-type :cart-items)
    (primary-key :cid :merchant-id)
    (expires-in 1800))
  
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
  (adi :set-all! {:client-time "1221231222" :timezone "480"})
  
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
    (is (consumer :valid-key? :cid))
    (is (consumer :valid-key? :cart-items))
    (is (thrown? RuntimeException (consumer :valid-key? :another)))
    (is (= (consumer :primary-key) '(:cid :merchant-id)))
    (is (= (consumer :string-keys ["abcdef" "14"]) '("abcdef___14___:cid" "abcdef___14___:merchant-id" "abcdef___14___:session-start-time" "abcdef___14___:url-referrer" "abcdef___14___:client-time" "abcdef___14___:timezone")))
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

    (is (thrown? RuntimeException (adi :set! :blah 123)))
    (is (thrown? RuntimeException (adi :get :blah 123)))
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
    (:value (((fetchers key-type) key) key)))

  (deftest test-copying-content
    (let [anya (consumer :new)]
      (anya :copy-from-humpty adi :cid :merchant-id)
      (is (= (adi :get :cid) (anya :get :cid)))
      (is (= (adi :get :merchant-id) (anya :get :merchant-id)))))

  (deftest test-real-saving
    (redis/flushdb)
    (adi :save!)
    (is (= (fetch-for-test :string-type "abcdef___14___:cid") "\"abcdef\""))
    (is (= (fetch-for-test :string-type "abcdef___14___:merchant-id") "\"14\""))
    (is (= (fetch-for-test :string-type "abcdef___14___:url-referrer") "\"google.com\""))
    (is (= (fetch-for-test :string-type "abcdef___14___:session-start-time") (str start-time)))
    (is (= (fetch-for-test :string-type "abcdef___14___:client-time") "\"1221231222\""))
    (is (= (fetch-for-test :string-type "abcdef___14___:timezone") "\"480\""))
    (is (= (fetch-for-test :list-type "abcdef___14___:cart-items") ["{:cost 22.4, :sku \"RST\"}" "{:cost 10.95, :sku \"XYZ\"}"]))
    (is (consumer :exists? "abcdef" "14"))
    (is (consumer :attrib-exists? :session-start-time "abcdef" "14")))

  (def now-score-string "20100129164026")
  (def now-score-date (score-date now-score-string))

  (deftest test-real-thawing
    (redis/flushdb)
    (stubbing [now-score now-score-string]
      (adi :save!))
    (let [new-adi (consumer :find "abcdef" "14")]
      (is (= (new-adi :get :cid) "abcdef"))
      (is (= (new-adi :get :merchant-id) "14"))
      (is (= (new-adi :get :session-start-time) start-time))
      (is (= (new-adi :get :url-referrer) "google.com"))
      (is (= (count (new-adi :get :cart-items)) 2))
      (is (= (new-adi :get :cart-items) (apply list [item-2 item-1])))
      (is (= (new-adi :last-updated) now-score-date))))

  (deftest test-finding-nothing
    (let [new-adi (consumer :find "blah" "deblah")]
      (is (nil? new-adi))))

  (deftest test-destroy
    (consumer :destroy "ady" "15")
    (let [ady (consumer :new)
          number-keys (count (redis/keys "*"))]
      (ady :set-all! {:cid "ady" :merchant-id "15" :timezone "420"})
      (ady :save!)
      (is (not (nil? (consumer :find "ady" "15"))))
      (is (= (count (redis/keys "*")) (+ 3 number-keys)))
      (is (not (nil? (redis/zscore LAST-ACCESSED-TIMES "ady___15"))))
      (consumer :destroy "ady" "15")
      (is (nil? (consumer :find "ady" "15")))
      (is (= (count (redis/keys "*")) number-keys))
      (is (nil? (redis/zscore LAST-ACCESSED-TIMES "ady___15")))))

  (deftest test-expiration-true
    (let [ady (consumer :new)]
      (ady :set-all! {:cid "ady" :merchant-id "15" :timezone "420"})
      (stubbing [now-score now-score-string]
        (ady :save!))
      (is (ady :expired?))))

  (deftest test-expiration-false
    (let [ady (consumer :new)]
      (ady :set-all! {:cid "ady" :merchant-id "15" :timezone "420"})
      (ady :save!)
      (is (not (ady :expired?)))))


) ;; outer binding form

;

(defn run-humpty-dumpty-tests 
  ([]
     (binding [*redis-server-spec* redis-server-spec]
       (redis/with-server redis-server-spec (run-tests))))
  ([test-fn]
     (binding [*redis-server-spec* redis-server-spec]
       (redis/with-server redis-server-spec (test-fn))))) 

