use crate::CubeError;
use async_trait::async_trait;
use std::any::{type_name, TypeId};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::raw::TraitObject;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct Injector {
    services: RwLock<HashMap<String, Arc<RwLock<Option<Arc<dyn DIService>>>>>>,
    factories: RwLock<
        HashMap<
            String,
            Box<
                dyn Fn(Arc<Injector>) -> Pin<Box<dyn Future<Output = Arc<dyn DIService>> + Send>>
                    + Send
                    + Sync,
            >,
        >,
    >,
}

#[async_trait]
pub trait InjectorRef: Send + Sync {
    async fn get_service<T: ?Sized + Send + Sync + 'static>(&self, name: &str) -> Arc<T>;
    async fn get_service_typed<T: ?Sized + Send + Sync + 'static>(&self) -> Arc<T>;
    async fn has_service<T: ?Sized + Send + Sync + 'static>(&self, name: &str) -> bool;
    async fn has_service_typed<T: ?Sized + Send + Sync + 'static>(&self) -> bool;
}

impl Injector {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            services: RwLock::new(HashMap::new()),
            factories: RwLock::new(HashMap::new()),
        })
    }

    pub async fn register_typed_with_default<I: ?Sized, T, F, FF>(&self, factory: FF)
    where
        FF: FnOnce(Arc<Injector>) -> F + Send + Sync + Clone + 'static,
        F: Future<Output = Arc<T>> + Send,
        T: DIService + 'static,
    {
        self.register_typed::<T, T, F, FF>(factory).await;
        self.register_typed::<I, T, _, _>(async move |i: Arc<Injector>| {
            i.get_service_typed::<T>().await
        })
        .await;
    }

    pub async fn register_typed<I: ?Sized, T, F, FF>(&self, factory: FF)
    where
        FF: FnOnce(Arc<Injector>) -> F + Send + Sync + Clone + 'static,
        F: Future<Output = Arc<T>> + Send,
        T: DIService + 'static,
    {
        let name = type_name::<I>().to_string();
        self.factories.write().await.insert(
            name.to_string(),
            Box::new(move |i| {
                let fn_to_move = factory.clone();
                Box::pin(async move {
                    let arc: Arc<dyn DIService> = fn_to_move(i.clone()).await;
                    arc
                })
            }),
        );
        self.services
            .write()
            .await
            .insert(name, Arc::new(RwLock::new(None)));
    }

    pub async fn register<F>(
        &self,
        name: &str,
        factory: impl FnOnce(Arc<Injector>) -> F + Send + Sync + Clone + 'static,
    ) where
        F: Future<Output = Arc<dyn DIService>> + Send,
    {
        self.factories.write().await.insert(
            name.to_string(),
            Box::new(move |i| {
                let fn_to_move = factory.clone();
                Box::pin(async move { fn_to_move(i.clone()).await })
            }),
        );
        self.services
            .write()
            .await
            .insert(name.to_string(), Arc::new(RwLock::new(None)));
    }
}

pub async fn get_service<T: ?Sized + Send + Sync + 'static>(
    injector: &Arc<Injector>,
    name: &str,
) -> Arc<T> {
    if injector
        .services
        .read()
        .await
        .get(name)
        .unwrap()
        .read()
        .await
        .is_none()
    {
        let service_opt_lock = {
            let map_lock = injector.services.read().await;
            map_lock.get(name).unwrap().clone()
        };
        // println!("Locking service: {}", name);
        // TODO cycle depends lead to dead lock here
        let mut service_opt = service_opt_lock.write().await;
        if service_opt.is_none() {
            let factories = injector.factories.read().await;
            let factory = factories
                .get(name)
                .expect(&format!("Service not found: {}", name));
            let service = factory(injector.clone()).await;
            // println!("Setting service: {}", name);
            *service_opt = Some(service);
        }
    }
    let map_lock = injector.services.read().await;
    let opt_lock = map_lock.get(name).unwrap();
    let arc = opt_lock
        .read()
        .await
        .as_ref()
        .expect("Unexpected state")
        .clone();
    arc.downcast::<T>(arc.clone()).unwrap()
}

pub async fn get_service_typed<T: ?Sized + Send + Sync + 'static>(
    injector: &Arc<Injector>,
) -> Arc<T> {
    get_service(injector, type_name::<T>()).await
}

#[async_trait]
impl InjectorRef for Arc<Injector> {
    async fn get_service<T: ?Sized + Send + Sync + 'static>(&self, name: &str) -> Arc<T> {
        get_service(self, name).await
    }

    async fn get_service_typed<T: ?Sized + Send + Sync + 'static>(&self) -> Arc<T> {
        get_service_typed(self).await
    }

    async fn has_service<T: ?Sized + Send + Sync + 'static>(&self, name: &str) -> bool {
        self.factories.read().await.contains_key(name)
    }

    async fn has_service_typed<T: ?Sized + Send + Sync + 'static>(&self) -> bool {
        self.factories.read().await.contains_key(type_name::<T>())
    }
}

pub trait DIService: Send + Sync {
    fn downcast_ref(
        &self,
        target: TypeId,
        type_name: &'static str,
        arc: Arc<dyn DIService>,
    ) -> Result<TraitObject, CubeError>;
}

impl dyn DIService {
    pub fn downcast<T: ?Sized + 'static>(
        &self,
        arc: Arc<dyn DIService>,
    ) -> Result<Arc<T>, CubeError> {
        unsafe {
            let obj = self.downcast_ref(TypeId::of::<T>(), type_name::<T>(), arc)?;
            let ptr = *(&obj as *const TraitObject as *const &T);
            Ok(Arc::from_raw(ptr))
        }
    }
}

#[macro_export]
macro_rules! di_service (
    ( $ty: ident, [ $( $trait_ty: ident ),* ]) => {
        impl crate::config::injection::DIService for $ty {
            fn downcast_ref(
                &self,
                target: core::any::TypeId,
                type_name: &'static str,
                arc: Arc<dyn crate::config::injection::DIService>,
            ) -> Result<core::raw::TraitObject, CubeError> {
                unsafe {
                    let ptr = Arc::into_raw(arc);
                    let arc = Arc::<Self>::from_raw(ptr as *const Self);
                    $(
                    if target == core::any::TypeId::of::<dyn $trait_ty>() {
                        let iface_arc: Arc<dyn $trait_ty> = arc;
                        let ptr = Arc::into_raw(iface_arc);
                        return Ok(std::mem::transmute(&*ptr));
                    }
                    )*
                    if target == core::any::TypeId::of::<$ty>() {
                        let typ_arc: Arc<$ty> = arc;
                        let ptr = Arc::into_raw(typ_arc);
                        return Ok(core::raw::TraitObject {
                            data: ptr as *const _ as *mut (),
                            vtable: std::ptr::null_mut(),
                        });
                    }
                }
                Err(CubeError::internal(format!(
                    "Can't cast service to {:?}",
                    type_name
                )))
            }
        }
    }
);
