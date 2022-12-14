// ----------------------------------------------------------------------

export type ActionMapType<M extends { [index: string]: any }> = {
  [Key in keyof M]: M[Key] extends undefined
    ? {
        type: Key;
      }
    : {
        type: Key;
        payload: M[Key];
      };
};

export type AuthUserType = null | Record<string, any>;

export type AuthStateType = {
  isAuthenticated: boolean;
  isInitialized: boolean;
  user: AuthUserType;
};

// ----------------------------------------------------------------------

export type Auth0ContextType = {
  method: 'auth0';
  isAuthenticated: boolean;
  isInitialized: boolean;
  user: AuthUserType;
  login: () => Promise<void>;
  logout:  () => Promise<void>;
};