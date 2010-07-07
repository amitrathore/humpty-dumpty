(ns org.rathore.amit.humpty-dumpty.core-spec
  (:use [clojure.test :only [run-tests deftest is testing]])
  (:use org.rathore.amit.humpty-dumpty.core)
  (:use org.rathore.amit.humpty-dumpty.persistence)
  (:use org.rathore.amit.humpty-dumpty.utils)
  (:use org.rathore.amit.conjure.core))

(def redis-server-spec {:host "127.0.0.1" :db 8})

(defdumpty consumer
    (string-type :cid :merchant-id :session-start-time :url-referrer :client-time :timezone)
    (list-type :cart-items)
    (map-type :info )
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
  (adi :set-all! {:client-time "1221231222" :timezone "480"})
  
  (def item-1 {:cost 10.95 :sku "XYZ"})
  (def item-2 {:cost 22.40 :sku "RST"})
  (adi :add! :cart-items item-1)
  (adi :add! :cart-items item-2)

  (def info {:name "adi" :age 6})
  (adi :set! :info info)

  (deftest test-override-spec
    (is (= (consumer-json :format) :json))
    (is (= (consumer-json :key-separator) "***")))

  (deftest test-validate-for-persistence
    (let [new-consumer (consumer :new)]
      (is (thrown? RuntimeException (validate-for-persistence new-consumer)))))

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
    (is (= (adi :primary-key-values) ["abcdef" "14"]))
    (is (= (adi :get :cid) "abcdef"))
    (is (= (adi :get :merchant-id) "14"))
    (is (= (adi :get :session-start-time) start-time))
    (is (= (adi :get :url-referrer) "google.com"))
    
    (is (= (count (adi :get :cart-items)) 2))
    (is (= (adi :get :cart-items) (apply list [item-2 item-1])))

    (is (= (adi :get :info) info))

    (is (thrown? RuntimeException (adi :set! :blah 123)))
    (is (thrown? RuntimeException (adi :get :blah 123))))

  
  (deftest test-copying-content
    (let [anya (consumer :new)]
      (anya :copy-from-humpty adi :cid :merchant-id)
      (is (= (adi :get :cid) (anya :get :cid)))
      (is (= (adi :get :merchant-id) (anya :get :merchant-id)))))

  
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

      (is (= (new-adi :get-all :cid :merchant-id :session-start-time) {:cid "abcdef" :merchant-id "14" :session-start-time start-time}))

      (is (= (count (new-adi :get :cart-items)) 2))
      (is (= (new-adi :get :cart-items) (apply list [item-2 item-1])))

      (is (= (new-adi :get :info) info))))

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
      (is (= (count (redis/keys "*")) (+ 1 number-keys)))
      (consumer :destroy "ady" "15")
      (is (nil? (consumer :find "ady" "15")))
      (is (= (count (redis/keys "*")) number-keys))))

  (deftest test-exists
    (testing "should return true when it is present"
      (let [ady (consumer :new)]
        (ady :set-all! {:cid "ady" :merchant-id "15"})
        (ady :save!)
        (is (consumer :exists? "ady" "15")))
      (consumer :destroy "ady" "15"))
    (testing "should return false when it is not present"
      (is (not (consumer :exists? "ady" "15")))))

  (deftest test-update-field
    (let [ady (consumer :new)]
      (ady :set-all! {:cid "ady" :merchant-id "15" :timezone "480" :url-referrer "google.com"})
      (ady :save!)
      (let [new-adi (consumer :find "ady" "15")]
        (is (= (new-adi :get :url-referrer) "google.com"))
        (is (= (new-adi :get :timezone) "480"))
        (new-adi :update-values! {:url-referrer "yahoo.com" :timezone "560"  :info {:name "ady"}})
        (let [newer-adi (consumer :find "ady" "15")]
          (is (= (newer-adi :get :cid) "ady"))
          (is (= (newer-adi :get :url-referrer) "yahoo.com"))
          (is (= (newer-adi :get :timezone) "560"))
          (is (= (newer-adi :get :info) {:name "ady"}))))))



(defn run-humpty-dumpty-tests 
  ([]
     (redis/with-server redis-server-spec (run-tests)))
  ([test-fn]
     (redis/with-server redis-server-spec (test-fn)))) 

