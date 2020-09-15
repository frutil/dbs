(ns frutil.dbs.databases
  (:require
   [datahike.api :as d]

   [frutil.dbs.files :as files]))


(defonce DB-CONNECTIONS (atom {}))


(defn storage-path []
  "app-data/dbs")


(defn db-path [db-ident]
  (str (storage-path) "/" (namespace db-ident) "/" (name db-ident)))


(defn db-config [db-ident]
  {:store {:backend :file
           :path (str (db-path db-ident) "/datahike")}})


(defn create [db-ident owners-user-id]
  (let [cfg (db-config db-ident)
        cfg (assoc cfg :initial-tx [{:db/ident :db.user/id
                                     :db/unique :db.unique/identity
                                     :db/valueType :db.type/string
                                     :db/cardinality :db.cardinality/one}
                                    {:db/ident :db.user/owner?
                                     :db/valueType :db.type/boolean
                                     :db/cardinality :db.cardinality/one}
                                    {:db.user/id owners-user-id
                                     :db.user/owner? true}])
        dir (-> cfg :store :path java.io.File. .getAbsoluteFile)]
    (when (-> dir .exists)
      (throw (ex-info (str "Database already exists: " db-ident)
                      {:db-ident db-ident
                       :dir dir})))
    (print "Creating database:" db-ident dir)
    (-> dir .mkdirs)
    (d/create-database cfg)))


(defn databases-idents []
  (reduce
   (fn [dbs namespace-dir]
     (if (-> namespace-dir .isDirectory)
       (reduce
        (fn [dbs name-dir]
          (if (-> namespace-dir .isDirectory)
            (conj dbs (keyword (-> namespace-dir .getName) (-> name-dir .getName)))
            dbs))
        dbs (-> namespace-dir .listFiles))
       dbs))
   #{} (-> (storage-path) java.io.File. .listFiles)))


(defn new-db-connection [db-ident]
  (print "Connecting database:" db-ident)
  (let [conn (d/connect (db-config db-ident))]
    (swap! DB-CONNECTIONS assoc db-ident conn)
    conn))


(defn db-connection [db-ident]
  (locking DB-CONNECTIONS
    (if-let [conn (get @DB-CONNECTIONS db-ident)]
      conn
      (new-db-connection db-ident))))


(defn release-db-connection [db-ident]
  (locking DB-CONNECTIONS
    (when-let [conn (get @DB-CONNECTIONS db-ident)]
      (d/release conn)
      (swap! DB-CONNECTIONS dissoc db-ident))))


(defn delete [db-ident]
  (print "Deleting database:" db-ident)
  (release-db-connection db-ident)
  (let [cfg (db-config db-ident)]
    (d/delete-database cfg)
    (let [dir (db-path db-ident)]
      (files/delete dir)
      (-> dir java.io.File. .getParentFile .delete))))


(defn query [db-ident q]
  (d/q q @(db-connection db-ident)))


(defn entities-data [db-ident wheres]
  (let [db @(db-connection db-ident)]
    (->> db
         (d/q (into '[:find ?e :where [?e :db/ident _]] wheres))
         (map first)
         (map (partial d/entity db))
         (mapv #(select-keys % (keys %))))))


(defn transact [db-ident tx-request]
  (d/transact (db-connection db-ident) tx-request))


;;; repl

(comment
  (def db-ident :witek/test)
  (databases-idents)
  (create :witek/test "witek")
  (delete :witek/test)
  (db-connection :witek/test)

  (query :witek/test '[:find ?e ?id :where [?e :db.user/id ?id]])
  (transact :witek/test [{:db.user/id "kacper"
                          :db.user/meta {:hello :world}}])

  (d/q '[:find ?e ?id
         :where
         [?e :db.user/id ?id]]
       @(db-connection :witek/test))
  (d/q '[:find ?e ?id
         :where
         [?e :db.user/owner? true]
         [?e :db.user/id ?id]]
       @(db-connection :witek/test))
  (d/transact (db-connection :witek/test)
              [{:db.user/id "hogi"
                :db.user/owner? false}])

  (def ids (query :witek/test '[:find ?e :where [?e :db/ident _]]))
  (map #(d/entity @(db-connection :witek/test) (first %))
       ids))
