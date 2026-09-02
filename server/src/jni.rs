use crate::{ServerAuth, ServerConfig, ServerHandle};
use jni::Env;
use jni::EnvUnowned;
use jni::objects::{JClass, JObject, JString};
use jni::sys::jstring;
use once_cell::sync::Lazy;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Mutex;

static SERVER_HANDLE: Lazy<Mutex<Option<ServerHandle>>> = Lazy::new(|| Mutex::new(None));

#[cfg(target_os = "android")]
fn init_android_tls_verifier(env: &mut Env, context: JObject) -> jni::errors::Result<()> {
    rustls_platform_verifier::android::init_with_env(env, context)
}

#[cfg(not(target_os = "android"))]
fn init_android_tls_verifier(_env: &mut Env, _context: JObject) -> jni::errors::Result<()> {
    Ok(())
}

/// The configuration `startServerNative` runs with: [`ServerConfig::embedded`]
/// bound to loopback on `port`, logging to `config_dir`.
///
/// The control API runs with [`ServerAuth::Disabled`] here. The JNI surface
/// returns only the base URL, so the Kotlin side -- and the stremio-core it
/// drives -- has no way to receive a per-launch token; with a generated one
/// every control call would get a 401. The listener is loopback-only, so the
/// threat model is exactly what it was before the bearer token existed: any
/// process on the device could already reach the port. Keep this in step with
/// `stremio-android`; the JNI symbols must stay stable.
fn jni_config(config_dir: PathBuf, cache_dir: PathBuf, port: u16) -> ServerConfig {
    let mut cfg = ServerConfig::embedded();
    cfg.config_dir = Some(config_dir);
    cfg.cache_dir = Some(cache_dir);
    cfg.http_addr = SocketAddr::from(([127, 0, 0, 1], port));
    cfg.init_logging = true;
    cfg.auth = ServerAuth::Disabled;
    cfg
}

#[unsafe(no_mangle)]
/// Starts the embedded server from a JVM native call.
///
/// # Safety
///
/// The JNI environment and object handles must be valid for the duration of
/// this call and must originate from the invoking JVM thread.
pub unsafe extern "C" fn Java_com_stremio_mobile_server_JniStreamingServerController_startServerNative(
    mut env: EnvUnowned,
    _class: JClass,
    context: JObject,
    config_dir: JString,
    cache_dir: JString,
    port: jni::sys::jint,
) -> jstring {
    env.with_env(|env| -> jni::errors::Result<jstring> {
        init_android_tls_verifier(env, context)?;

        let config_dir_str: String = match config_dir.try_to_string(env) {
            Ok(s) => s,
            Err(_) => {
                let _ = env.throw_new(
                    jni::jni_str!("java/lang/IllegalArgumentException"),
                    jni::jni_str!("Invalid configDir string"),
                );
                return Ok(std::ptr::null_mut());
            }
        };

        let cache_dir_str: String = match cache_dir.try_to_string(env) {
            Ok(s) => s,
            Err(_) => {
                let _ = env.throw_new(
                    jni::jni_str!("java/lang/IllegalArgumentException"),
                    jni::jni_str!("Invalid cacheDir string"),
                );
                return Ok(std::ptr::null_mut());
            }
        };

        let mut handle_lock = SERVER_HANDLE.lock().unwrap();
        if handle_lock.is_some() {
            let bound_addr = handle_lock.as_ref().unwrap().bound_http_addr();
            let url = format!("http://{}", bound_addr);
            return Ok(env.new_string(url)?.into_raw());
        }

        let cfg = jni_config(
            PathBuf::from(config_dir_str),
            PathBuf::from(cache_dir_str),
            port as u16,
        );

        match crate::start(cfg) {
            Ok(handle) => {
                let bound_addr = handle.bound_http_addr();
                let url = format!("http://{}", bound_addr);
                *handle_lock = Some(handle);
                Ok(env.new_string(url)?.into_raw())
            }
            Err(err) => {
                let err_msg = format!("Failed to start server: {}", err);
                let jni_err_msg = jni::strings::JNIString::from(err_msg);
                let _ = env.throw_new(jni::jni_str!("java/lang/RuntimeException"), jni_err_msg);
                Ok(std::ptr::null_mut())
            }
        }
    })
    .resolve::<jni::errors::ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
/// Stops the embedded server from a JVM native call.
///
/// # Safety
///
/// The JNI environment and class handle must originate from the invoking JVM
/// thread.
pub unsafe extern "C" fn Java_com_stremio_mobile_server_JniStreamingServerController_stopServerNative(
    _env: EnvUnowned,
    _class: JClass,
) {
    let mut handle_lock = SERVER_HANDLE.lock().unwrap();
    if let Some(handle) = handle_lock.take() {
        let _ = handle.shutdown();
        let _ = handle.join();
    }
}

#[unsafe(no_mangle)]
/// Returns the embedded server URL to a JVM native caller.
///
/// # Safety
///
/// The JNI environment and class handle must be valid for the duration of this
/// call and must originate from the invoking JVM thread.
pub unsafe extern "C" fn Java_com_stremio_mobile_server_JniStreamingServerController_getServerUrlNative(
    mut env: EnvUnowned,
    _class: JClass,
) -> jstring {
    env.with_env(|env| -> jni::errors::Result<jstring> {
        let handle_lock = SERVER_HANDLE.lock().unwrap();
        if let Some(handle) = handle_lock.as_ref() {
            let bound_addr = handle.bound_http_addr();
            let url = format!("http://{}", bound_addr);
            Ok(env.new_string(url)?.into_raw())
        } else {
            Ok(std::ptr::null_mut())
        }
    })
    .resolve::<jni::errors::ThrowRuntimeExAndDefault>()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The JNI embedder cannot receive a token (the native call returns only
    /// the URL), so its server must run the control API open; everything else
    /// is the embedded default plus the two directories and the loopback port.
    #[test]
    fn jni_config_disables_auth_on_a_loopback_listener() {
        let cfg = jni_config(
            PathBuf::from("/data/config"),
            PathBuf::from("/data/cache"),
            11470,
        );
        assert_eq!(cfg.auth, ServerAuth::Disabled);
        assert_eq!(cfg.http_addr, SocketAddr::from(([127, 0, 0, 1], 11470)));
        assert!(cfg.http_addr.ip().is_loopback());
        assert_eq!(
            cfg.config_dir.as_deref(),
            Some(std::path::Path::new("/data/config"))
        );
        assert_eq!(
            cfg.cache_dir.as_deref(),
            Some(std::path::Path::new("/data/cache"))
        );
        assert!(cfg.init_logging);
        assert_eq!(
            ServerConfig::embedded().auth,
            ServerAuth::Generated,
            "the JNI path opts out; the embedded default keeps its token"
        );
    }
}
