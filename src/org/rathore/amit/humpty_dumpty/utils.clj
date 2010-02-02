(ns org.rathore.amit.humpty-dumpty.utils
  (:import (java.util Calendar GregorianCalendar)
           (java.text SimpleDateFormat)))

(defn padded-str [val]
  (let [val-str (str val)]
    (if (= 1 (count val-str))
      (str "0" val-str)
      val-str)))

(defn month-str [greg]
  (padded-str (inc (.get greg Calendar/MONTH))))

(defn padded-str-for [greg field]
  (padded-str (.get greg field)))

(defn now-score 
  ([now-greg]
     (let [year (.get now-greg Calendar/YEAR)
           month (month-str now-greg)
           day (padded-str-for now-greg Calendar/DATE)
           hour (padded-str-for now-greg Calendar/HOUR_OF_DAY)
           minute (padded-str-for now-greg Calendar/MINUTE)
           seconds (padded-str-for now-greg Calendar/SECOND)]
       (apply str [year month day hour minute seconds])))
  ([]
     (now-score (GregorianCalendar.))))

(defn score-date [score]
  (let [date (.parse (SimpleDateFormat. "yyyyMMddkkmmss") score)
        cal (Calendar/getInstance)]
    (.setTime cal date)
    cal))

(defn add-seconds [cal seconds]
  (.add cal Calendar/SECOND seconds)
  cal)
