(ns frutil.dbs.files
  (:require
   [clojure.java.io :as io]))


(defn delete [path]
  (let [file (io/as-file path)]
    (when (-> file .exists)
      (when (-> file .isDirectory)
        (doseq [file (-> file .listFiles)]
          (delete file)))
      (io/delete-file file))))
