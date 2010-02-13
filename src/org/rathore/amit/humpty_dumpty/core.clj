;; a humpty is the object that will be persisted into redis
;; a dumpty is the blue-print for a humpty

(ns org.rathore.amit.humpty-dumpty.core
  (:require redis)
  (use clojure.contrib.str-utils)
  (use org.rathore.amit.humpty-dumpty.persistence))

(def *redis-server-spec*)

(defn primary-key-value [humpty-obj]
  (let [pk-keys ((humpty-obj :type) :primary-key)
	separator ((humpty-obj :type) :key-separator)
	values (map #(humpty-obj :get %) pk-keys)]
    (str-join separator values)))

(defn new-humpty [dumpty]
  (let [state (ref {})]
    (fn thiz [accessor & args]
      (redis/with-server *redis-server-spec*
	(condp = accessor

	  :type dumpty

          :type-name (dumpty :name)

	  :set! (let [[k v] args] 
		  (dumpty :valid-key? k)
		  (dosync
		   (alter state assoc k v))
		  v)

	  :set-all! (let [[kv-map] args]
		      (doseq [kv kv-map]
			(let [[k v] kv]
			  (thiz :set! k v))))

	  :copy-from-humpty (let [from (first args)
				  attribs (rest args)]
			      (doseq [attrib attribs]
				(thiz :set! attrib (from :get attrib))))

	  :add! (let [[k v] args
		      add-to-inner-list (fn [current-state ke valu] 
					  (update-in current-state [ke] conj valu))]
		  (dosync
		   (alter state add-to-inner-list k v)))

	  :get (let [[k] args]
		 (dumpty :valid-key? k)
		 (state k))

          :get-all (apply merge (map (fn [k] {k (state k)}) args))

	  :primary-key-value (primary-key-value thiz)

	  :save! (persist thiz)

          :update-values! (let [[key-vals] args]
                            (thiz :set-all! key-vals)
                            (persist thiz key-vals))

	  :get-state @state

	  :replace-state (let [[new-state] args] 
			   (dosync
			    (ref-set state new-state)))

          :last-updated (get-last-updated thiz)

          :expired? (humpty-expired? thiz))))))

(defn key-type-for [key-name string-types list-types]
  (if (some #(= % key-name) string-types) 
    :string-type
    (if (some #(= % key-name) list-types)
      :list-type)))

(defn keys-for [keys separator values]
  (let [pk-value (str-join separator values)]
    (map #(str pk-value separator %) keys)))

(defn check-key-validity [key dumpty string-attribs list-attribs]
  (if-not (some #(= % key) string-attribs)
    (if-not (some #(= % key) list-attribs)
      (throw (RuntimeException. (str "Attempt to use unknown key " key " in object of humpty type " (dumpty :name))))))
  true)

(defn new-dumpty [name separator format expires-in primary-keys string-attribs list-attribs]
  (fn dumpty [accessor & args]
    (redis/with-server *redis-server-spec*
      (condp = accessor

	:name name

	:format format

        :ttl expires-in

	:key-separator separator

	:primary-key primary-keys

	:key-type (let [[k] args]
		    (key-type-for k string-attribs list-attribs))

	:valid-key? (let [[key] args]
		      (check-key-validity key dumpty string-attribs list-attribs))

	:string-keys (let [[values] args] 
		       (keys-for string-attribs separator values))

	:list-keys (let [[values] args]
		     (keys-for list-attribs separator values))

	:new (new-humpty dumpty)

	:new-with-state (let [[new-state] args
			      nh (new-humpty dumpty)]
			  (nh :replace-state new-state)
			  nh)

	:find (find-by-primary-key dumpty args)

	:exists? (let [key-value (str-join separator args)
                       key-value (str key-value separator (first primary-keys))]
		  (redis/exists key-value))

        :expired? (dumpty-expired? dumpty args)

	:attrib-exists? (let [attrib-key (first args)
			      pk-value (str-join separator (rest args))]
			  (redis/exists (str pk-value separator attrib-key)))

        :destroy (destroy-by-primary-key dumpty args)))))

(defn specs-for [redis-datatype specs]
  (let [type-spec? #(= redis-datatype (first %))
	extractor (comp next first)]
    (extractor (filter type-spec? specs))))

(defmacro defdumpty [name & specs]
  (let [string-types (specs-for 'string-type specs)
	list-types (specs-for 'list-type specs)
	pk-keys (specs-for 'primary-key specs)
	format (or (first (specs-for 'format-type specs)) :clj-str)
        expires-in (first (specs-for 'expires-in specs))
	separator (or (first (specs-for 'key-separator specs)) "___")]
    `(def ~name 
	  (new-dumpty '~name ~separator ~format (or ~expires-in 0) '~pk-keys '~string-types '~list-types))))
