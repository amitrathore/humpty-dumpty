(ns org.rathore.amit.humpty-dumpty.utils
  (:import (java.util Calendar GregorianCalendar)
           (java.text SimpleDateFormat)))

(defn month-str [greg]
  (let [val (str (inc (.get greg Calendar/MONTH)))]
    (if (= 1 (count val))
      (str "0" val)
      val)))

(defn now-score []
  (let [now-greg (GregorianCalendar.)
        year (.get now-greg Calendar/YEAR)
        month (month-str now-greg)
        day (.get now-greg Calendar/DATE)
        hour (.get now-greg Calendar/HOUR_OF_DAY)
        minute (.get now-greg Calendar/MINUTE)
        seconds (.get now-greg Calendar/SECOND)]
    (apply str [year month day hour minute seconds ".0"])))

(defn score-date [score]
  (.parse (SimpleDateFormat. "yyyyMMddkkmmss") score))