;; a humpty is the object that will be persisted into redis
;; a dumpty is the blue-print for a humpty

(ns org.rathore.amit.humpty-dumpty.core
  (:require redis))

(def *redis-server-spec* {})

(defn new-humpty [dumpty]
  (let [state (ref {})]
    (fn thiz [accessor & args]
      (redis/with-server *redis-server-spec*
	(cond
	  (= :type accessor) dumpty
	  (= :set! accessor) (let [[k v] args] 
			       (dosync
				(alter state assoc k v))
			       v)
	  (= :add! accessor) (let [[k v] args
				   add-to-inner-list (fn [current-state ke valu] 
						       (update-in current-state [ke] conj valu))]
			       (dosync
				(alter state add-to-inner-list k v)))
	  (= :get accessor) (let [[k] args]
			      (state k))
	  :else (throw (RuntimeException. (str "Unknown message " accessor " sent to humpty-dumpty object of type " (dumpty :name)))))))))

(defn new-dumpty [name format string-attribs set-attribs]
  (fn dumpty [accessor & args]
    (redis/with-server *redis-server-spec*
      (cond
	(= :name accessor) name
	(= :format accessor) format
	(= :new accessor) (new-humpty dumpty)
	:else (throw (RuntimeException. (str "Unknown commmand " accessor " sent to " name)))))))

(defn specs-for [redis-datatype specs]
  (let [type-spec? #(= redis-datatype (first %))
	extractor (comp next first)]
    (extractor (filter type-spec? specs))))

(defmacro defdumpty [name & specs]
  (let [string-types (specs-for 'string-type specs)
	list-types (specs-for 'list-type specs)
	format (or (first (specs-for 'format-type specs)) :clj-str)]
    `(def ~name 
	  (new-dumpty '~name '~format '~string-types '~list-types))))
