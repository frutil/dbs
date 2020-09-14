(ns frutil.dbs.http-server
  (:require
   [clojure.edn :as edn]
   [clojure.pprint :refer [pprint]]

   [ring.util.response :as response]

   [org.httpkit.server :as httpkit]
   [reitit.ring :as ring]
   [reitit.swagger :as swagger]
   [reitit.swagger-ui :as swagger-ui]
   [reitit.ring.coercion :as coercion]
   [reitit.coercion.malli]
   [reitit.ring.middleware.muuntaja :as muuntaja]
   [reitit.ring.middleware.exception :as exception]
   [reitit.ring.middleware.multipart :as multipart]
   [reitit.ring.middleware.parameters :as parameters]
   [reitit.ring.middleware.dev]
   [reitit.dev.pretty :as pretty]
   [muuntaja.core :as m]

   [frutil.logging.core :as l]
   [frutil.dbs.files :as files]
   [frutil.dbs.databases :as databases]))


(defn list-databases [{{{:keys []} :query} :parameters}]
  {:status 200
   :body (into [] (map (fn [db-ident] {:ident db-ident
                                       :label (str db-ident)})
                       (databases/databases-idents)))})


(defn create-database [{{{:keys [id]} :path} :parameters}]
  (println "create" id)
  (databases/create id "root")
  {:status 200
   :body {}})


(defn delete-database [{{{:keys [id]} :path} :parameters}]
  (println "delete" id)
  (databases/delete id)
  {:status 200
   :body {}})


(defn query-database [{{{:keys [id]} :path {:keys [q]} :query} :parameters}]
  (println "query" id (type q) q)
  {:status 200
   :body (databases/query id q)})


(defn transact-database [{{{:keys [id]} :path {:keys [tx]} :query} :parameters}]
  (println "transact" id tx)
  {:status 200
   :body (-> (databases/transact id tx) :tx-data)})


;;; console


(def console-dev-path "../dbs-console")

(defn console [req]
  (let [resource (-> req :path-params :resource)
        resource (if (= resource "")
                   "index.html"
                   resource)
        file (files/file-from resource
                              [(str console-dev-path "/resources/public")
                               (str console-dev-path "/target/public")])]
    (response/file-response (-> file .getCanonicalPath))))


;;; middleware


(def exception-middleware
  (exception/create-exception-middleware
   (merge
    exception/default-handlers
    {::exception/wrap (fn [handler e request]
                        (l/log :error/request-handler-failed
                               :handler handler
                               :exception e
                               :request request)
                        (handler e request))})))


;;; app

(def app
  (ring/ring-handler
    (ring/router
      [["/swagger.json"
        {:get {:no-doc true
               :swagger {:info {:title "Frankenburg Database"}
                         :basePath "/"} ;; prefix for all paths
               :handler (swagger/create-swagger-handler)}}]

       ["/api"

        ["/databases"
         {:swagger {:tags ["databases"]}}
         ["/"
          {:get {:summary "list all databases"
                 :responses {200 {:body vector?}}
                 :handler list-databases}}]
         ["/:id"
          {
           :get {:summary "query a database"
                 :parameters {:path [:map [:id qualified-keyword?]]
                              :query [:map
                                      [:q [:vector
                                           {:swagger/type "string"
                                            :decode/string edn/read-string}
                                           any?]]]}
                 :responses {200 {:body set?}}
                 :handler query-database}
           :put {:summary "transact a database"
                 :parameters {:path [:map [:id qualified-keyword?]]
                              :query [:map
                                      [:tx [:vector
                                            {:swagger/type "string"
                                             :decode/string edn/read-string}
                                            any?]]]}
                 :responses {200 {:body any?}}
                 :handler transact-database}
           :post {:summary "create a database"
                  :parameters {:path [:map [:id qualified-keyword?]]}
                  :responses {200 {:body any?}}
                  :handler create-database}
           :delete {:summary "delete a database"
                    :parameters {:path [:map [:id qualified-keyword?]]}
                    :responses {200 {:body any?}}
                    :handler delete-database}}]]]

       ["/console/*resource" console]]
                      ;; {:loader (-> "/p/frutil/dbs-console/target/public")})]]
                      ;;                   ;(java.net.URLClassLoader []})]]


      {:data {:coercion reitit.coercion.malli/coercion
              :muuntaja m/instance
              :middleware [
                           parameters/parameters-middleware
                           muuntaja/format-negotiate-middleware
                           muuntaja/format-response-middleware
                           ;;exception/exception-middleware
                           exception-middleware
                           muuntaja/format-request-middleware
                           coercion/coerce-response-middleware
                           coercion/coerce-request-middleware
                           coercion/coerce-exceptions-middleware
                           multipart/multipart-middleware]}
       ;:reitit.middleware/transform reitit.ring.middleware.dev/print-request-diffs
       :exception pretty/exception})

    (ring/routes
      (swagger-ui/create-swagger-ui-handler {:path "/swagger"})
      (ring/create-default-handler))))


(defonce SERVER (atom nil))


(defn stop []
  (when-not (nil? @SERVER)
    (@SERVER :timeout 100)
    (reset! SERVER nil)))


(defn start [port]
  ;(log/info :msg :starting :port port)
  (reset! SERVER (httpkit/run-server #'app {:port port})))


(defn restart []
  (stop)
  (start 23010))


(comment
  (restart))
